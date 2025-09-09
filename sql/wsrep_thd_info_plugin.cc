/* Copyright (C) 2025 Codership Oy <info@galeracluster.com>

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; either version 2
   of the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
   USA.
*/

#include "my_global.h"
#include "mysqld_error.h"
#include <mysql/plugin.h>
#include "sql_plugin.h"
#include "sql_priv.h"
#include "sql_class.h"
#include "set_var.h"
#include "sql_acl.h"
#include "sql_i_s.h"
#include "wsrep_mysqld.h"
#include "wsrep_i_s_helper.h"
#include <stdint.h>

/*
   WSREP_THD_INFO
 */
enum thd_info_columns {
  STATE_COLUMN_ID = 0,
  STATE_COLUMN_OS_THREAD_ID,
  STATE_COLUMN_MODE,
  STATE_COLUMN_STATE,
  STATE_COLUMN_TRANSACTION_ID,
  STATE_COLUMN_TRANSACTION_STATE,
  STATE_COLUMN_SEQNO,
  STATE_COLUMN_DEPENDS_ON,
  STATE_COLUMN_GTID
};

namespace Show {
static ST_FIELD_INFO wsrep_thd_info_fields[]=
{
  Column ("ID", ULong(), NOT_NULL),
  Column ("OS_THREAD_ID", ULonglong(), NOT_NULL),
  Column ("MODE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("STATE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("TRANSACTION_ID", ULonglong(), NULLABLE),
  Column ("TRANSACTION_STATE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("SEQNO", ULonglong(), NULLABLE),
  Column ("DEPENDS_ON", ULonglong(), NULLABLE),
  Column ("GTID", Varchar(FN_REFLEN), NULLABLE),
  CEnd()
};
} // namespace Show

static int wsrep_thd_info_plugin_deinit(void *p)
{
  return 0;
}

/**
   Function to fill information_schema.wsrep_thd_info

   @param thd THD handle
   @param table Handle to information_schema.wsrep_thd_info
   @return 0 ok
 */
static my_bool fill_wsrep_thd_info(THD* thd, TABLE *table)
{
  /* If thread is in killed state, do not add it to I_S table,
     because schema_table_store_record sends thd->send_kill_message()
     to I_S query causing ER_QUERY_INTERRUPTED error for query. */
  if (thd->killed == KILL_QUERY || thd->killed == KILL_QUERY_HARD)
    return 0;

  Field** fields= table->field;

  WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_ID], (long long)thd->thread_id));
  WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_OS_THREAD_ID], (long long)thd->real_id));
  WSREP_IS_OK(store_string(fields[STATE_COLUMN_MODE], wsrep_thd_client_mode_str(thd)));
  WSREP_IS_OK(store_string(fields[STATE_COLUMN_STATE], wsrep_thd_client_state_str(thd)));
  WSREP_IS_OK(store_string(fields[STATE_COLUMN_TRANSACTION_STATE], wsrep_thd_transaction_state_str(thd)));
  WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_TRANSACTION_ID], (long long)wsrep_thd_transaction_id(thd)));
  WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_SEQNO], (long long)wsrep_thd_trx_seqno(thd)));
  WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_DEPENDS_ON], (long long)wsrep_thd_depends_on(thd)));
  char gtid_str[FN_REFLEN];
  wsrep_thd_gtid_str(thd, gtid_str);
  WSREP_IS_OK(store_string(fields[STATE_COLUMN_GTID], gtid_str));
  WSREP_IS_OK(schema_table_store_record(thd, table));

  return 0;
}

/**
   Function to fill information_schema.wsrep_thd_info

   @param thd THD handle
   @param tables Handle to information_schema.wsrep_thd_info
   @param Ignored
   @return 1 error, 0 ok
 */
static int wsrep_thd_info_fill_view(THD *thd, TABLE_LIST *tables, Item *)
{
  TABLE *table = tables->table;

  // deny access to non-superusers
  if (check_global_access(thd, PROCESS_ACL))
    return 0;

  // If wsrep is disabled this table does not exists
  if (!WSREP_ON)
  {
    my_error(ER_NO_SUCH_TABLE, MYF(0), tables->db.str, tables->table_name.str);
    return 1;
  }

  server_threads.iterate(fill_wsrep_thd_info, table);
  return 0;
}

static int wsrep_thd_info_plugin_init(void *ptr)
{
  ST_SCHEMA_TABLE *schema_table= static_cast<ST_SCHEMA_TABLE *>(ptr);
  schema_table->fields_info = Show::wsrep_thd_info_fields;
  schema_table->fill_table = wsrep_thd_info_fill_view;
  return 0;
}

static struct st_mysql_information_schema plugin_descriptor =
{
  MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION
};

maria_declare_plugin(wsrep_thd_info)
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN, // plugin type
  &plugin_descriptor,
  "wsrep_thd_info",                // plugin name
  "Codership Oy",                  // author
  "Provides information about Galera thread state",
  PLUGIN_LICENSE_GPL,              // license
  wsrep_thd_info_plugin_init,      // init function
  wsrep_thd_info_plugin_deinit,    // deinit function
  0x0100,                          // version
  nullptr,                         // status variables
  nullptr,                         // system variables
  PACKAGE_VERSION,                 // version string
  MariaDB_PLUGIN_MATURITY_STABLE   // maturity
}
maria_declare_plugin_end;
