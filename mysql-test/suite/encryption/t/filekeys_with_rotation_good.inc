-- source include/have_innodb.inc
-- source filekeys_plugin.inc

--eval create table t1(c1 bigint not null, b char(200))  engine=innodb encrypted=yes encryption_key_id=$GOOD_ID
show create table t1;
insert t1 values (12345, repeat('1234567890', 20));

--error ER_ILLEGAL_HA_CREATE_OPTION
--eval alter table t1 encryption_key_id=$BAD_ID
show warnings;
show create table t1;

drop table t1;
