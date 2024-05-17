# Spider ODBC feature

### Abstract
---
-   Spider ODBC feature makes Spider possible to access data nodes through ODBC connections.

### Requirement
---
-   This feature is available from MariaDB Enterprise Server 10.5
-   Currently, this feature is only used with UnixODBC.
-   Spider have to be built with -DSPIDER_WITH_UNIXODBC=ON

### How to use
---
For using MariaDB/MySQL as a data node through ODBC, set the following parameter to Spider table system variable or CREATE/ALTER SERVER command.
-   wrapper "odbc_mariadb"

*NOTE: odbc_mariadb has been deprecated (MENT-1521).*

For using other databases as a data node through ODBC, set the following parameter to Spider table system variable or CREATE/ALTER SERVER command.
-   wrapper "odbc"

### An example to use PostgreSQL as a data node
---
On PostgreSQL (data node)
```
CREATE DATABASE test;
\c test
CREATE TABLE "t1" (
  "c1" INT,
  "c2" VARCHAR(255),
  "c3" TIMESTAMP,
  CONSTRAINT "pk_t1" PRIMARY KEY("c1")
);
SET DATESTYLE='ISO, YMD';
INSERT INTO "t1" VALUES (10, 'abCDEf012', '2020-04-22 09:20:11');
```

On MariaDB (Spider node)
```
CREATE DATABASE test;
USE test;
CREATE TABLE `t1` (
  `c1` INT,
  `c2` VARCHAR(255),
  `c3` DATETIME,
  PRIMARY KEY(`c1`)
)ENGINE=SPIDER COMMENT='dsn "YOURDSN", table "t1", wrapper "odbc"';
INSERT INTO `t1` VALUES (15, 'abCDEf012', '2020-04-20 10:10:10');
```

On PostgreSQL (data node)
```
test=# select * from t1;
 c1 |    c2     |         c3
----+-----------+---------------------
 10 | abCDEf012 | 2020-04-22 09:20:11
 15 | abCDEf012 | 2020-04-20 10:10:10
(2 rows)

test=# select avg(c1), max(c3) from t1;
         avg         |         max
---------------------+---------------------
 12.5000000000000000 | 2020-04-22 09:20:11
(1 row)
```

On MariaDB (Spider node)
```
MariaDB [test]> select * from t1;
+----+-----------+---------------------+
| c1 | c2        | c3                  |
+----+-----------+---------------------+
| 10 | abCDEf012 | 2020-04-22 09:20:11 |
| 15 | abCDEf012 | 2020-04-20 10:10:10 |
+----+-----------+---------------------+
2 rows in set (0.70 sec)

MariaDB [test]> select avg(c1), max(c3) from t1;
+---------+---------------------+
| avg(c1) | max(c3)             |
+---------+---------------------+
| 12.5000 | 2020-04-22 09:20:11 |
+---------+---------------------+
1 row in set (0.64 sec)
```

### An example to use ODBC data nodes as shards
---
On MariaDB (Spider node)
```
CREATE TABLE `t1` (
  `c1` INT,
  `c2` VARCHAR(255),
  `c3` DATETIME,
  PRIMARY KEY(`c1`)
)ENGINE=SPIDER COMMENT='table "t1", wrapper "odbc"'
PARTITION BY KEY(c1) (
  PARTITION p1 COMMENT='dsn "YOURDSN1"',
  PARTITION p2 COMMENT='dsn "YOURDSN2"',
  PARTITION p3 COMMENT='dsn "YOURDSN3"',
  ...
);
```

### Parameters for ODBC connections
---
The following parameters are available for ODBC connections.
-   filedsn: ODBC dsn file name
-   dsn: ODBC DSN
-   driver: ODBC driver name
-   host: ODBC server
-   socket: socket file name
-   port: ODBC port
-   database: ODBC database
-   user: ODBC UID
-   password: ODBC PWD (user is required for using this parameter)

Additionally, ssl_ca, ssl_capath, ssl_cert, ssl_cipher, ssl_key can be used for adding specific parameters of ODBC drivers.
-   ssl_ca is added on top of ODBC connection parameters.
-   ssl_capath is added between DSN and ODBC DRIVER.
-   ssl_cert is added between ODBC PORT and DATABASE.
-   ssl_cipher is added between ODBC DATABASE and UID.
-   ssl_key is added on bottom of ODBC connection parameters.

### Special treatment when the driver is MariaDB
---
There are some internal differences.

As an example, let t1 be a spider table with two remote tables (HA) on
two servers t11 (on SERVER s1) and t21 (on SERVER s2), and t2 one with
remotes t12 (on SERVER s1) and t22 (on SERVER s2). Assume also that s1
is defined to use database d1 and s2 is defined to use database d2.

First, consider the statement `LOCK TABLES t1 READ, t2 READ;`.

It is translated to two statements with the mariadb driver:

```
lock table `d1`.`t11` read,`d1`.`t12` read
lock table `d2`.`t21` read,`d2`.`t22` read
```

It is translated to six statements for non-mariadb drivers (not
necessarily double quotes as the quotes is obtained from the driver
through SQLGetInfo):

```
start transaction
lock table "t11" in share mode
lock table "t12" in share mode
start transaction
lock table "t21" in share mode
lock table "t22" in share mode
```

From this example we see that

- with the mariadb driver the modes of lock table are "read local",
  "read", "low_priority write", and "write", and with a non-mariadb
  driver the modes of lock table are "in share mode", and "in
  exclusive mode".
- with the mariadb driver the locking is done at once on all tables on
  the same server, and with a non-mariadb driver the lock is done one
  by one
- with a non-mariadb driver, the locking on each server is prefaced
  with a start transaction

Now, consider the statement `UNLOCK TABLES;`. With the mariadb driver,
it translates to one `unlock tables` statement for each server, and
with a non-mariadb driver, it translates to one `commit` statement for
each server.

Other differences:

-   mariadb driver:
    -   The shared lock word with SELECT is "lock in shared mode".
    -   The name of cursors have to change for each thread.
-   other drivers:
    -   The shared lock word with SELECT is "for share".
    -   The name of cursors can use same name for different threads.

### Limitations
---
-   XA transaction is not supported through ODBC connections.
-   Fulltext search is not supported through ODBC connections yet.
-   Pushdown index hint is not supported through ODBC connections yet.
-   Pushdown join is not supported through ODBC connections yet.
-   Index name is not seen when a duplicate entry is detected through an ODBC connection.
-   Table discovery is not supported through ODBC connections yet.

