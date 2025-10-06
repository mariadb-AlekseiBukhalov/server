-- Copyright (c) 2025, MariaDB
--
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; version 2 of the License.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1335 USA

DROP PACKAGE IF EXISTS dbms_output;

SET @old_sql_mode=@@sql_mode;
SET sql_mode=ORACLE;

DELIMITER $$
CREATE DEFINER='mariadb.sys'@'localhost' PACKAGE dbms_output
  SQL SECURITY INVOKER
AS
  -- buffer overflow (min 2000, def 20000, max unlimited)
  -- line length overflow (max 32767)
  PROCEDURE DISABLE;
  PROCEDURE ENABLE(buffer_size IN INTEGER DEFAULT 20000); -- NULL - no limit
  /*
    Return a single line excluding the final newline character.
    status: 0 means success, 1 means no more lines
    After calling GET_LINE or GET_LINES, any lines not retrieved
    before the next call to PUT, PUT_LINE, or NEW_LINE are discarded
    to avoid confusing them with the next message.
    GET_LINE does not return lines that have not been terminated with a
    newline character.
  */
  PROCEDURE GET_LINE(line OUT TEXT CHARACTER SET utf8mb4,
                     status OUT INTEGER);
  -- PROCEDURE GET_LINES; -- Not yet
  PROCEDURE GET_LINES_RESULT(); -- MariaDB extension
  PROCEDURE NEW_LINE;
  /*
    Put a partial line
    Exceptions:
    - Buffer overflow, limit of <buf_limit> bytes.
    - Line length overflow, limit of 32767 bytes for each line.
  */
  PROCEDURE PUT(item IN TEXT CHARACTER SET utf8mb4);
  PROCEDURE PUT_LINE(item IN TEXT CHARACTER SET utf8mb4);
END
$$

CREATE DEFINER='mariadb.sys'@'localhost' PACKAGE BODY dbms_output
  SQL SECURITY INVOKER
AS
  TYPE buffer_t IS TABLE OF TEXT CHARACTER SET utf8mb4 INDEX BY INTEGER;

  m_buffer buffer_t;

  /*
    The size of the data in the buffer.
  */
  m_buffer_size BIGINT UNSIGNED;
  /*
    If ENABLED() has been called.
  */
  m_enabled BOOL;
  /*
    Maximum allowed buffer size.
  */
  m_max_buffer_size BIGINT UNSIGNED;
  /*
    If the previous command was PUT() rather than PUT_LINE().
    In this case the next PUT*() should append the previous line in the buffer
    rather than start a new line.
  */
  m_continue_line BOOL;
  /*
    If the previous command was GET_LINE().
    The next PUT*() should clear the buffer then:
    If a call for GET_LINE() did not retrieve all lines,
    then a subsequent call for PUT(), PUT_LINE(), or NEW_LINE() discards
    the remaining lines (to avoid confusing with the next message).
  */
  m_get_started BOOL;

  PROCEDURE RAISE_TOO_LONG_LINE AS
  BEGIN
    SIGNAL SQLSTATE '22001' SET MYSQL_ERRNO = 1406,
      MESSAGE_TEXT = 'Data length is too long for DBMS_OUTPUT.PUT_LINE()';
  END;

  PROCEDURE RAISE_BUFFER_SIZE_OVERFLOW AS
  BEGIN
    SIGNAL SQLSTATE '22001' SET MYSQL_ERRNO = 1406,
      MESSAGE_TEXT = 'DBMS_OUTPUT buffer size overflow';
  END;

  -- buffer overflow (min 2000, def 20000, max unlimited)
  -- line length overflow (max 32767)
  PROCEDURE DISABLE AS
  BEGIN
    m_enabled:= FALSE;
    m_max_buffer_size:= 20000;
    m_continue_line:= FALSE;
    m_get_started:= FALSE;
    m_buffer.delete;
    m_buffer_size:= 0;
  END;

  -- buffer=NULL - no limit
  PROCEDURE ENABLE(buffer_size IN INTEGER DEFAULT 20000) AS
  BEGIN
    m_enabled:= TRUE;
    m_max_buffer_size:= buffer_size;
    IF m_max_buffer_size < 2000
    THEN
      m_max_buffer_size:= 2000;
    END IF;
    IF m_max_buffer_size > 1000000
    THEN
      m_max_buffer_size:= 1000000;
    END IF;
    m_continue_line:= FALSE;
    m_get_started:= FALSE;
    m_buffer.delete;
    m_buffer_size:= 0;
  END;

  /*
    Return a single line excluding the final newline character.
    status: 0 means success, 1 means no more lines
    After calling GET_LINE or GET_LINES, any lines not retrieved
    before the next call to PUT, PUT_LINE, or NEW_LINE are discarded
    to avoid confusing them with the next message.
    GET_LINE does not return lines that have not been terminated with a
    newline character.
  */
  PROCEDURE GET_LINE(line OUT TEXT CHARACTER SET utf8mb4,
                     status OUT INTEGER) AS
    idx INTEGER:= m_buffer.first;
  BEGIN
    m_get_started:= TRUE;
    IF idx IS NULL THEN
      line:= NULL;
      status:= 1;
      m_buffer_size:= 0;
    ELSE
      line:= m_buffer(idx);
      m_buffer_size:= m_buffer_size - OCTET_LENGTH(COALESCE(line,''));
      m_buffer.delete(idx);
      status:= 0;
    END IF;
  END;

  FUNCTION RETURN_LINE() RETURN TEXT CHARACTER SET utf8mb4 AS
    line TEXT CHARACTER SET utf8mb4;
    status INT;
  BEGIN
    CALL sys.DBMS_OUTPUT.GET_LINE(line, status);
    RETURN line;
  END;

  PROCEDURE GET_LINES_RESULT AS
    cnt INT := m_buffer.count;
  BEGIN
    WITH RECURSIVE cte AS (
        SELECT 0 AS idx, RETURN_LINE() AS line WHERE cnt > 0
      UNION ALL
        SELECT idx + 1, RETURN_LINE()
        FROM cte
        WHERE idx + 1 < cnt
    )
    SELECT line FROM cte;
  END;

  PROCEDURE NEW_LINE AS
  BEGIN
    m_continue_line:= FALSE;
  END;

  /*
    Put a partial line
    Exceptions:
    - Buffer overflow, limit of <buf_limit> bytes.
    - Line length overflow, limit of 32767 bytes for each line.
  */
  PROCEDURE PUT(item IN TEXT CHARACTER SET utf8mb4) AS
  BEGIN
    IF NOT m_enabled
    THEN
      RETURN;
    END IF;
    PUT_LINE(item);
    m_continue_line:= TRUE;
  END;

  PROCEDURE PUT_LINE(item IN TEXT CHARACTER SET utf8mb4) AS
    idx INTEGER:= m_buffer.last;
    item_octet_length INTEGER;
  BEGIN
    IF NOT m_enabled
    THEN
      RETURN;
    END IF;
    item_octet_length:= OCTET_LENGTH(COALESCE(item,''));
    IF m_get_started
    THEN
      m_buffer.delete;
      m_buffer_size:= 0;
      m_get_started:= FALSE;
    END IF;
    IF item_octet_length > 32767
    THEN
      RAISE_TOO_LONG_LINE();
      RETURN;
    END IF;
    IF m_max_buffer_size IS NOT NULL AND
       m_buffer_size + item_octet_length > m_max_buffer_size
    THEN
      RAISE_BUFFER_SIZE_OVERFLOW();
      RETURN;
    END IF;
    IF idx IS NULL
    THEN
      m_buffer(0):= item;
      m_buffer_size:= item_octet_length;
    ELSIF m_continue_line
    THEN
      IF OCTET_LENGTH(m_buffer(idx)) + item_octet_length > 32767
      THEN
        RAISE_TOO_LONG_LINE();
        RETURN;
      END IF;
      m_buffer(idx):= m_buffer(idx) || item;
      m_buffer_size:= m_buffer_size + item_octet_length;
    ELSE
      m_buffer(idx + 1):= item;
      m_buffer_size:= m_buffer_size + item_octet_length;
    END IF;
    m_continue_line:= FALSE;
  END;
BEGIN
  m_enabled:= FALSE;
END
$$
DELIMITER ;

SET sql_mode=@old_sql_mode;
