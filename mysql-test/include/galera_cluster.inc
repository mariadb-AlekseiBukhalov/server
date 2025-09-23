# galera_cluster.inc
# ==================
#
# Description
# -----------
# Configure galera cluster with $galera_cluster_size (default: 2) nodes.
#
# The script sets the following helper variables:
# $galera_have_grp - Non-zero if Galera Plugin is used
#

if (!$galera_cluster_size)
{
#  --die ERROR IN TEST: $galera_cluster_size variable must be set
  --let $galera_cluster_size = 2
}

--source include/galera_init.inc
--source include/galera_wait_ready.inc

--let $_galera_node= $galera_cluster_size

while ($_galera_node != 1)
{
  --connection node_$_galera_node
  --source include/galera_wait_ready.inc
  --source include/have_innodb.inc

  --dec $_galera_node
}

--connection node_1
--let $galera_have_grp = `SELECT COUNT(*) = 1 FROM information_schema.global_variables WHERE VARIABLE_NAME = "wsrep_provider" AND VARIABLE_VALUE = "grp"`
