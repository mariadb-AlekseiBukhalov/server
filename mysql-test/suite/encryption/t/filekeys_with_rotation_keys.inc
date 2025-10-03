-- source include/have_innodb.inc
-- source filekeys_plugin.inc

if ($CLEANUP == 0)
{
    --eval create table t(c1 bigint not null, b char(200)) engine=innodb encrypted=yes encryption_key_id=$GOOD_ID
    insert t values (12345, repeat('678', 5));
}

if ($CLEANUP == 1)
{
    select * from t;
}

show create table t;

if ($CLEAN_UP == 1)
{
    drop table t;
}
