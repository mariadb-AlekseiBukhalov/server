-- source include/have_innodb.inc
-- source filekeys_plugin.inc

--eval call mtr.add_suppression("$SEARCH_PATTERN")
call mtr.add_suppression("Invalid key version");

--replace_result $MYSQL_TMP_DIR MYSQL_TMP_DIR
--error 2
FLUSH FILE_KEY_MANAGEMENT_KEYS;

--source include/search_pattern_in_file.inc

--let SEARCH_PATTERN=$FLUSH_FAIL
--source include/search_pattern_in_file.inc

--error ER_CANT_CREATE_TABLE
--eval create table t1(c1 bigint not null, b char(200))  engine=innodb encrypted=yes encryption_key_id=$KEY_ID
