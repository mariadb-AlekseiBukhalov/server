--source ../../../include/init_spider.inc

DELIMITER |;
CREATE PROCEDURE
  ORACLE_DROP_TABLE_IF_EXISTS (table_name TEXT) MODIFIES SQL DATA
  BEGIN
    SELECT spider_direct_sql(
      CONCAT(
        'BEGIN
          EXECUTE IMMEDIATE \'DROP TABLE ', '"', table_name, '"', '\';
        EXCEPTION
          WHEN OTHERS THEN
            IF sqlcode != -0942 THEN RAISE; END IF;
        END;'
      ),
      '',
      'dsn "ora", wrapper "odbc"'
    );
  END|
DELIMITER ;|
