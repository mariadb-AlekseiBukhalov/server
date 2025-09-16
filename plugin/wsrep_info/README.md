# Wsrep Info Plugin
The Wsrep Info plugin exposes the following information schema tables:
- WSREP_MEMBERSHIP
- WSREP_STATUS
- WSREP_CERT_KEYS
- WSREP_CERT_KEYS_HISTORY


## WSREP_MEMBERSHIP
The WSREP_MEMBERSHIP table shows Galera node cluster membership information
in the INFORMATION_SCHEMA database.

> SELECT * FROM information_schema.WSREP_MEMBERSHIP;
+-------+--------------------------------------+-------+-----------------+
| INDEX | UUID                                 | NAME  | ADDRESS         |
+-------+--------------------------------------+-------+-----------------+
|     0 | 36hs88s2-1d2b-32a1-23b6-d763bd7356c7 | node1 | 10.0.1.11:90000 |
|     1 | 732ki44s-1d2b-32a1-85be-77ae3792d2a9 | node2 | 10.0.1.11:90001 |
|     2 | 6773ik85-1d2b-32a1-76gd-9d727ab24cb1 | node3 | 10.0.1.11:90002 |
+-------+--------------------------------------+-------+-----------------+


## WSREP_STATUS
The WSREP_STATUS table shows Galera node cluster status information through
WSREP_STATUS table in the INFORMATION_SCHEMA database.

> SELECT * FROM information_schema.WSREP_STATUS\G
*************************** 1. row ***************************
         NODE_INDEX: 0
        NODE_STATUS: Synced
     CLUSTER_STATUS: Primary
       CLUSTER_SIZE: 3
 CLUSTER_STATE_UUID: 372ab79a-2a5c-27b2-91cb-23ab3830b1a2
CLUSTER_STATE_SEQNO: 2
    CLUSTER_CONF_ID: 3
                GAP: NO
   PROTOCOL_VERSION: 3


## WSREP_CERT_KEYS
The WSREP_CERT_KEYS table exposes the current wsrep certification keys for the
transaction as information schema table. The certification keys for a
transaction are added to handle conflict resolution while applying concurrent
transactions.

> SELECT * FROM INFORMATION_SCHEMA.WSREP_CERT_KEYS;
+-------+----------------------+-----------+
| THD_ID| KEY_STRING           | KEY_TYPE  |
+-------+----------------------+-----------+
|    10 | 74657374 7431 0001   | exclusive |
|    10 | 74657374 7431 010002 | exclusive |
+-------+----------------------+-----------+


## WSREP_CERT_KEYS_HISTORY
The WSREP_CERT_KEYS_HISTORY table exposes the most recent (maximum 1024 keys)
certification keys added for the thread THD as information schema
table. The certification keys for a transaction are added to
handle conflict resolution while applying concurrent transactions.

> SELECT * FROM INFORMATION_SCHEMA.WSREP_CERT_KEYS_HISTORY;
+-------+----------------------+-----------+
| THD_ID| KEY_STRING           | KEY_TYPE  |
+-------+----------------------+-----------+
|    10 | 74657374 7431        | exclusive |
|    10 | 74657374 7431        | exclusive |
|    10 | 74657374 7431 0001   | exclusive |
|    10 | 74657374 7431 010002 | exclusive |
+-------+----------------------+-----------+
