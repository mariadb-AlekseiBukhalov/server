/* Copyright (C) 2014 MariaDB Corporation.
   Copyright (C) 2025 Codership Oy

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA */

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include <my_global.h>
#include <mysql/plugin.h>
#include <sql_i_s.h>                            /* ST_SCHEMA_TABLE */
#include <sql_class.h>                          /* server_threads */
#include <sql_show.h>
#include <sql_acl.h>                            /* check_global_access() */
#include <wsrep_mysqld.h>
#include <wsrep_utils.h>
#include <wsrep_i_s_helper.h>
#include <wsrep_thd_info_history.h>
#include <wsrep_bf_abort_history.h>
#include <string>
#include <deque>
#include <mutex>

#include "wsrep_cert_key_history.h"

/* WSREP_MEMBERSHIP table fields */

/* Node index */
#define COLUMN_WSREP_MEMB_INDEX 0
/* Unique member ID */
#define COLUMN_WSREP_MEMB_UUID 1
/* Human-readable name */
#define COLUMN_WSREP_MEMB_NAME 2
/* Incoming address */
#define COLUMN_WSREP_MEMB_ADDRESS 3

/* WSREP_STATUS table fields */

/* Node index */
#define COLUMN_WSREP_STATUS_NODE_INDEX 0
/* Node status */
#define COLUMN_WSREP_STATUS_NODE_STATUS 1
/* Cluster status */
#define COLUMN_WSREP_STATUS_CLUSTER_STATUS 2
/* Cluster size */
#define COLUMN_WSREP_STATUS_CLUSTER_SIZE 3
/* Global cluster state UUID */
#define COLUMN_WSREP_STATUS_CLUSTER_STATE_UUID 4
/* Global cluster state Sequence number */
#define COLUMN_WSREP_STATUS_CLUSTER_STATE_SEQNO 5
/* Cluster membership changes */
#define COLUMN_WSREP_STATUS_CLUSTER_CONF_ID 6
/* Application protocol version */
#define COLUMN_WSREP_STATUS_PROTO_VERSION 7

/*
   WSREP_THD_INFO_HISTORY
 */
enum thd_info_history_columns {
  STATE_COLUMN_ID= 0,
  STATE_COLUMN_OS_THREAD_ID,
  STATE_COLUMN_TIME,
  STATE_COLUMN_MODE,
  STATE_COLUMN_STATE,
  STATE_COLUMN_TRANSACTION_ID,
  STATE_COLUMN_TRANSACTION_STATE,
  STATE_COLUMN_SEQNO,
  STATE_COLUMN_DEPENDS_ON,
  STATE_COLUMN_GTID
};

/*
  WSREP_BF_ABORT_HISTORY
*/

enum bf_abort_history_columns {
  BFA_TIME= 0,
  BFA_VICTIM_TRX_ID,
  BFA_VICTIM_THREAD_ID,
  BFA_VICTIM_QUERY_ID,
  BFA_VICTIM_SEQNO,
  BFA_VICTIM_CLIENT_STATE,
  BFA_VICTIM_CLIENT_MODE,
  BFA_VICTIM_TRX_STATE,
  BFA_VICTIM_LOCK,
  BFA_BF_TRX_ID,
  BFA_BF_THREAD_ID,
  BFA_BF_QUERY_ID,
  BFA_BF_SEQNO,
  BFA_BF_CLIENT_STATE,
  BFA_BF_CLIENT_MODE,
  BFA_BF_TRX_STATE,
  BFA_BF_LOCK,
  BFA_SPACE_ID,
  BFA_PAGE_NO,
  BFA_INDEX_NAME,
  BFA_TABLE_NAME
};

namespace Show {

static ST_FIELD_INFO wsrep_memb_fields[]=
{
  Column("INDEX",   SLong(),                        NOT_NULL, "Index"),
  Column("UUID",    Varchar(WSREP_UUID_STR_LEN),    NOT_NULL, "Uuid"),
  Column("NAME",    Varchar(WSREP_MEMBER_NAME_LEN), NOT_NULL, "Name"),
  Column("ADDRESS", Varchar(WSREP_INCOMING_LEN),    NOT_NULL, "Address"),
  CEnd()
};

static ST_FIELD_INFO wsrep_status_fields[]=
{
  Column("NODE_INDEX",          SLong(),     NOT_NULL, "Node_Index"),
  Column("NODE_STATUS",         Varchar(16), NOT_NULL, "Node_Status"),
  Column("CLUSTER_STATUS",      Varchar(16), NOT_NULL, "Cluster_Status"),
  Column("CLUSTER_SIZE",        SLong(),     NOT_NULL, "Cluster_Size"),
  Column("CLUSTER_STATE_UUID",  Varchar(WSREP_UUID_STR_LEN), NOT_NULL),
  Column("CLUSTER_STATE_SEQNO", SLonglong(), NOT_NULL),
  Column("CLUSTER_CONF_ID",     SLonglong(), NOT_NULL),
  Column("PROTOCOL_VERSION",    SLong(),     NOT_NULL),
  CEnd()
};

static ST_FIELD_INFO wsrep_thd_info_history_fields[]=
{
  Column ("ID", ULong(), NOT_NULL),
  Column ("OS_THREAD_ID", ULonglong(), NOT_NULL),
  Column ("TIME", Datetime(6), NOT_NULL),
  Column ("MODE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("STATE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("TRANSACTION_ID", ULonglong(), NULLABLE),
  Column ("TRANSACTION_STATE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("SEQNO", ULonglong(), NULLABLE),
  Column ("DEPENDS_ON", ULonglong(), NULLABLE),
  Column ("GTID", Varchar(FN_REFLEN), NULLABLE),
  CEnd()
};

static ST_FIELD_INFO wsrep_bf_aborts_fields_info[]=
{
  Column ("TIME", Datetime(6), NOT_NULL),
  Column ("VICTIM_TRX_ID", ULonglong(), NULLABLE),
  Column ("VICTIM_THREAD_ID", ULonglong(), NOT_NULL),
  Column ("VICTIM_QUERY_ID", ULonglong(), NOT_NULL),
  Column ("VICTIM_SEQNO", ULonglong(), NULLABLE),
  Column ("VICTIM_CLIENT_STATE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("VICTIM_CLIENT_MODE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("VICTIM_TRX_STATE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("VICTIM_LOCK", Varchar(WSREP_BFA_LEN), NULLABLE),
  Column ("BF_TRX_ID", ULonglong(), NULLABLE),
  Column ("BF_THREAD_ID", ULonglong(), NOT_NULL),
  Column ("BF_QUERY_ID", ULonglong(), NOT_NULL),
  Column ("BF_SEQNO", ULonglong(), NULLABLE),
  Column ("BF_CLIENT_STATE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("BF_CLIENT_MODE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("BF_TRX_STATE", Varchar(WSREP_THD_INFO_LEN), NULLABLE),
  Column ("BF_LOCK", Varchar(WSREP_BFA_LEN), NULLABLE),
  Column ("SPACE_ID", ULonglong(), NULLABLE),
  Column ("PAGE_NO", ULonglong(), NULLABLE),
  Column ("INDEX_NAME", Varchar(FN_REFLEN), NULLABLE),
  Column ("TABLE_NAME", Varchar(FN_REFLEN), NULLABLE),
  CEnd()
};

} // namespace Show


static int wsrep_memb_fill_table(THD *thd, TABLE_LIST *tables, COND *cond)
{
  int rc= 0;

  if (check_global_access(thd, SUPER_ACL, true))
    return rc;

  wsrep_config_state->lock();

  const wsrep::view& view(wsrep_config_state->get_view_info());
  const std::vector<wsrep::view::member>& members(view.members());


  TABLE *table= tables->table;

  for (unsigned int i= 0; i < members.size(); i++)
  {
    table->field[COLUMN_WSREP_MEMB_INDEX]->store(i, 0);

    std::ostringstream os;
    os << members[i].id();
    table->field[COLUMN_WSREP_MEMB_UUID]->store(os.str().c_str(),
                                                os.str().length(),
                                                system_charset_info);
    table->field[COLUMN_WSREP_MEMB_NAME]->store(members[i].name().c_str(),
                                                members[i].name().length(),
                                                system_charset_info);
    table->field[COLUMN_WSREP_MEMB_ADDRESS]->store(members[i].incoming().c_str(),
                                                   members[i].incoming().length(),
                                                   system_charset_info);

    if (schema_table_store_record(thd, table))
    {
      rc= 1;
      goto end;
    }
  }

end:
  wsrep_config_state->unlock();
  return rc;
}

static int wsrep_memb_plugin_init(void *p)
{
  ST_SCHEMA_TABLE *schema= (ST_SCHEMA_TABLE *)p;

  schema->fields_info= Show::wsrep_memb_fields;
  schema->fill_table= wsrep_memb_fill_table;

  return 0;
}

static struct st_mysql_information_schema wsrep_memb_plugin=
{ MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };

static int wsrep_status_fill_table(THD *thd, TABLE_LIST *tables, COND *cond)
{
  int rc= 0;

  if (check_global_access(thd, SUPER_ACL, true))
    return rc;

  wsrep_config_state->lock();

  const wsrep::view& view= wsrep_config_state->get_view_info();
  enum wsrep::server_state::state status= wsrep_config_state->get_status();

  TABLE *table= tables->table;

  table->field[COLUMN_WSREP_STATUS_NODE_INDEX]
    ->store(view.own_index(), 0);
  table->field[COLUMN_WSREP_STATUS_NODE_STATUS]
    ->store(to_c_string(status),
            strlen(to_c_string(status)),
            system_charset_info);
  table->field[COLUMN_WSREP_STATUS_CLUSTER_STATUS]
    ->store(to_c_string(view.status()),
            strlen(to_c_string(view.status())),
            system_charset_info);
  table->field[COLUMN_WSREP_STATUS_CLUSTER_SIZE]->store(view.members().size(), 0);

  std::ostringstream os;
  os << view.state_id().id();
  table->field[COLUMN_WSREP_STATUS_CLUSTER_STATE_UUID]
    ->store(os.str().c_str(), os.str().length(), system_charset_info);

  table->field[COLUMN_WSREP_STATUS_CLUSTER_STATE_SEQNO]
    ->store(view.state_id().seqno().get(), 0);
  table->field[COLUMN_WSREP_STATUS_CLUSTER_CONF_ID]
    ->store(view.view_seqno().get(), 0);
  table->field[COLUMN_WSREP_STATUS_PROTO_VERSION]
    ->store(view.protocol_version(), 0);

  if (schema_table_store_record(thd, table))
    rc= 1;

  wsrep_config_state->unlock();
  return rc;
}

static int wsrep_status_plugin_init(void *p)
{
  ST_SCHEMA_TABLE *schema= (ST_SCHEMA_TABLE *)p;

  schema->fields_info= Show::wsrep_status_fields;
  schema->fill_table= wsrep_status_fill_table;

  return 0;
}

static struct st_mysql_information_schema wsrep_status_plugin=
{ MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };

/*
  Plugin library descriptor
*/

/** Wsrep thd info history in-memory cache */
static std::deque<wsrep_thd_info_history_t> wsrep_thd_info_history;

/** Store thd info history from thd to in-memory cache

@param thd         thread handle
@param thd_end     called from thd destructor ?
*/
int wsrep_thd_info_aggregate(const THD* thd, bool thd_end)
{
  for (auto const& id : thd->wsrep_cs().thd_history())
  {
    try
    {
      wsrep_thd_info_history.push_back(id);
      if (wsrep_thd_info_history.size() > WSREP_THD_INFO_HISTORY_LEN)
        wsrep_thd_info_history.pop_front();
    }
    catch(...)
    {
      WSREP_DEBUG("Storing wsrep thd info to history cache failed.");
      return 1;
    }
  }

  return 0;
}

static my_bool wsrep_thd_info_history_aggregate(THD* thd, void* p __attribute__((unused)))
{
  std::lock_guard<std::mutex> wsrep_thd_mutex(wsrep_thd_info_mutex);
  return (wsrep_thd_info_aggregate(thd, false));
}

static int wsrep_thd_info_history_gather()
{
  server_threads.iterate(wsrep_thd_info_history_aggregate, (void*)NULL);
  return 0;
}

/**
   Function to fill information_schema.wsrep_thd_info_history

   @param thd THD handle
   @param tables Handle to information_schema.wsrep_thd_info_history
   @param Ignored
   @return 1 error, 0 ok
 */
static int fill_wsrep_thd_info_history(THD* thd, TABLE_LIST *tables, Item *)
{
  TABLE *table = tables->table;
  Field** fields= table->field;

  // deny access to non-superusers
  if (check_global_access(thd, PROCESS_ACL))
    return 0;

  // If wsrep is disabled this table does not exists
  if (!WSREP_ON)
  {
    my_error(ER_NO_SUCH_TABLE, MYF(0), tables->db.str, tables->table_name.str);
    return 1;
  }

  // From running threads aggregate current stored history
  wsrep_thd_info_history_gather();

  std::lock_guard<std::mutex> wsrep_thd_mutex(wsrep_thd_info_mutex);
  for (auto const& id : wsrep_thd_info_history)
  {
    WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_ID], id.thread_id));
    WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_OS_THREAD_ID], id.os_thread_id));
    WSREP_IS_OK(store_time(fields[STATE_COLUMN_TIME], id.state_time));
    WSREP_IS_OK(store_string(fields[STATE_COLUMN_MODE], id.cs_mode));
    WSREP_IS_OK(store_string(fields[STATE_COLUMN_STATE], id.cs_state));
    WSREP_IS_OK(store_string(fields[STATE_COLUMN_TRANSACTION_STATE], id.trx_state));
    WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_TRANSACTION_ID], id.trx_id));
    WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_SEQNO], id.seq_no));
    WSREP_IS_OK(store_longlong(fields[STATE_COLUMN_DEPENDS_ON], id.depends_on));
    WSREP_IS_OK(store_string(fields[STATE_COLUMN_GTID], id.gtid));
    WSREP_IS_OK(schema_table_store_record(thd, table));
  }
  return 0;
}

static int wsrep_thd_info_history_plugin_init(void *ptr)
{
  std::lock_guard<std::mutex> wsrep_thd_mutex(wsrep_thd_info_mutex);
  ST_SCHEMA_TABLE *schema_table= static_cast<ST_SCHEMA_TABLE *>(ptr);
  schema_table->fields_info= Show::wsrep_thd_info_history_fields;
  schema_table->fill_table= fill_wsrep_thd_info_history;
  wsrep_thd_info_aggregate_func= wsrep_thd_info_aggregate;

  return 0;
}

static int wsrep_thd_info_history_plugin_deinit(void *p)
{
  std::lock_guard<std::mutex> wsrep_thd_mutex(wsrep_thd_info_mutex);
  wsrep_thd_info_aggregate_func= nullptr;

  return 0;
}

static struct st_mysql_information_schema wsrep_thd_info_history_plugin =
{
  MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION
};

static struct st_mysql_information_schema wsrep_bf_abort_history_plugin =
{
  MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION
};

/** Mutex protecting in-memory cache of the wsrep bf aborts */
static std::mutex wsrep_bf_abort_mutex;

/** In-memory cache for the wsrep bf aborts */
static std::deque<wsrep_bf_abort_t> wsrep_bf_abort_history;

static int fill_wsrep_bf_aborts(THD *thd, TABLE_LIST *tables, Item *)
{
  // deny access to non-superusers
  if (check_global_access(thd, PROCESS_ACL))
    return 0;

  // If wsrep is disabled this table does not exists
  if (!WSREP_ON)
  {
    my_error(ER_NO_SUCH_TABLE, MYF(0), tables->db.str, tables->table_name.str);
    return 1;
  }

  Field** fields= tables->table->field;
  std::lock_guard<std::mutex> wsrep_bf_aborts_lock(wsrep_bf_abort_mutex);
  for (auto const& id : wsrep_bf_abort_history)
  {
    WSREP_IS_OK(store_time(fields[BFA_TIME], id.time));
    WSREP_IS_OK(store_ulong(fields[BFA_VICTIM_TRX_ID],id.victim_trx_id));
    WSREP_IS_OK(store_longlong(fields[BFA_VICTIM_THREAD_ID], id.victim_thread_id));
    WSREP_IS_OK(store_longlong(fields[BFA_VICTIM_QUERY_ID], id.victim_query_id));
    WSREP_IS_OK(store_longlong(fields[BFA_VICTIM_SEQNO], id.victim_seqno));
    WSREP_IS_OK(store_string(fields[BFA_VICTIM_CLIENT_STATE], id.victim_client_state));
    WSREP_IS_OK(store_string(fields[BFA_VICTIM_CLIENT_MODE], id.victim_client_mode));
    WSREP_IS_OK(store_string(fields[BFA_VICTIM_TRX_STATE], id.victim_trx_state));
    WSREP_IS_OK(store_ulong(fields[BFA_BF_TRX_ID], id.bf_trx_id));
    WSREP_IS_OK(store_longlong(fields[BFA_BF_THREAD_ID], id.bf_thread_id));
    WSREP_IS_OK(store_longlong(fields[BFA_BF_QUERY_ID], id.bf_query_id));
    WSREP_IS_OK(store_longlong(fields[BFA_BF_SEQNO], id.bf_seqno));
    WSREP_IS_OK(store_string(fields[BFA_BF_CLIENT_STATE], id.bf_client_state));
    WSREP_IS_OK(store_string(fields[BFA_BF_CLIENT_MODE], id.bf_client_mode));
    WSREP_IS_OK(store_string(fields[BFA_BF_TRX_STATE], id.bf_trx_state));
    WSREP_IS_OK(store_ulong(fields[BFA_SPACE_ID], id.space_id));
    WSREP_IS_OK(store_ulong(fields[BFA_PAGE_NO], id.page_no));
    WSREP_IS_OK(store_string(fields[BFA_INDEX_NAME], id.index_name));
    WSREP_IS_OK(store_string(fields[BFA_TABLE_NAME], id.table_name));
    WSREP_IS_OK(store_string(fields[BFA_VICTIM_LOCK], id.victim_lock));
    WSREP_IS_OK(store_string(fields[BFA_BF_LOCK], id.bf_lock));
    WSREP_IS_OK(schema_table_store_record(thd, tables->table));
  }

  return 0;
}

/** Store thd bf info to in-memory cache

@param thd         thread handle
@param bfinfo      bf abort info

@return 0 in success, 1 in case of failure
*/
int wsrep_bf_abort_info_store(THD *thd, const wsrep_bf_abort_t* bfinfo)
{
  std::lock_guard<std::mutex> wsrep_bf_abort_lock(wsrep_bf_abort_mutex);
  wsrep_bf_abort_t bf_info= *bfinfo;
  try
  {
    wsrep_bf_abort_history.push_back(bf_info);
    if (wsrep_bf_abort_history.size() > WSREP_BF_ABORT_HISTORY_LEN)
      wsrep_bf_abort_history.pop_front();
  }
  catch(...)
  {
    WSREP_DEBUG("Storing wsrep bf info to history cache failed.");
    return 1;
  }

  return 0;
}

/** Bind the dynamic table INFORMATION_SCHEMA.wsrep_bf_abort_history
@return 0 on success */
static int wsrep_bf_abort_history_plugin_init(void* p)
{
  ST_SCHEMA_TABLE* schema;

  std::lock_guard<std::mutex> wsrep_bf_abort_lock(wsrep_bf_abort_mutex);
  schema = (ST_SCHEMA_TABLE*) p;
  schema->fields_info= Show::wsrep_bf_aborts_fields_info;
  schema->fill_table= fill_wsrep_bf_aborts;
  wsrep_bf_abort_store_func= wsrep_bf_abort_info_store;
  wsrep_bf_abort_history_enabled= true;
  return 0;
}

static int wsrep_bf_abort_history_plugin_deinit(void *p)
{
  std::lock_guard<std::mutex> wsrep_bf_abort_lock(wsrep_bf_abort_mutex);
  wsrep_bf_abort_store_func= nullptr;
  wsrep_bf_abort_history_enabled= false;
  return 0;
}

/*
   WSREP_CERT_KEYS
*/
static struct st_mysql_information_schema wsrep_cert_keys_view = {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};

enum cert_keys_columns {
  CERT_KEYS_COLUMN_THD_ID = 0,
  CERT_KEYS_COLUMN_KEY_STRING,
  CERT_KEYS_COLUMN_KEY_TYPE
};

namespace Show {
static ST_FIELD_INFO wsrep_cert_keys_view_fields[] =
{
  Column("THD_ID", ULonglong(20), NOT_NULL, "Thread ID"),
  Column("KEY_STRING", Varchar(256), NOT_NULL, "Certification Key"),
  Column("KEY_TYPE", Varchar(64), NOT_NULL, "Certification Key Type"),
  CEnd()
};
} // namespace Show

static const char *wsrep_service_key_type_str(Wsrep_service_key_type key_type) {
  switch (key_type) {
    case WSREP_SERVICE_KEY_SHARED:
      return "shared";
    case WSREP_SERVICE_KEY_REFERENCE:
      return "reference";
    case WSREP_SERVICE_KEY_UPDATE:
      return "update";
    case WSREP_SERVICE_KEY_EXCLUSIVE:
      return "exclusive";
  }
  return "unknown";
}

struct wsrep_cert_keys_fill_func {
  void operator()(THD *thd) {
    if (!WSREP(thd)) {
      return;
    }
    struct call_ctx {
      THD *thd;
      TABLE *table;
    };
    call_ctx ctx = {thd, m_table};
    wsrep_cert_key_history_foreach(
        thd,
        [](void *ctx, my_thread_id thd_id, const char *key_string,
           Wsrep_service_key_type key_type) {
          call_ctx *c = (call_ctx *)ctx;
          c->table->field[CERT_KEYS_COLUMN_THD_ID]->store(thd_id, true);
          c->table->field[CERT_KEYS_COLUMN_KEY_STRING]->store(
              key_string, strlen(key_string), system_charset_info);
          c->table->field[CERT_KEYS_COLUMN_KEY_TYPE]->store(
              wsrep_service_key_type_str(key_type),
              strlen(wsrep_service_key_type_str(key_type)),
              system_charset_info);
          schema_table_store_record(c->thd, c->table);
          return true;
        },
        &ctx);
  }

  wsrep_cert_keys_fill_func(THD *caller_thd, TABLE *table)
      : m_caller_thd(caller_thd), m_table(table) {
  }

  THD *m_caller_thd;
  TABLE *m_table;
};


  static my_bool call_ctx_operator(THD *in_thd, wsrep_cert_keys_fill_func *arg) {
    if (!WSREP(in_thd)) {
      return true;
    }
    struct call_ctx {
      THD *thd;
      TABLE *table;
    };
    call_ctx ctx = {arg->m_caller_thd, arg->m_table};
    wsrep_cert_key_history_foreach(
        in_thd,
        [](void *ctx, my_thread_id thd_id, const char *key_string,
           Wsrep_service_key_type key_type) {
          call_ctx *c = (call_ctx *)ctx;
          c->table->field[CERT_KEYS_COLUMN_THD_ID]->store(thd_id, true);
          c->table->field[CERT_KEYS_COLUMN_KEY_STRING]->store(
              key_string, strlen(key_string), system_charset_info);
          c->table->field[CERT_KEYS_COLUMN_KEY_TYPE]->store(
              wsrep_service_key_type_str(key_type),
              strlen(wsrep_service_key_type_str(key_type)),
              system_charset_info);
          schema_table_store_record(c->thd, c->table);
          return true;
        },
        &ctx);
      return false;
  }

static int wsrep_cert_keys_fill_view(THD *thd, TABLE_LIST* tables, Item *) {
  TABLE *table = tables->table;
  wsrep_cert_keys_fill_func f(thd, table);
  server_threads.iterate(call_ctx_operator, &f);
  return 0;
}

static int wsrep_cert_keys_init(void *ptr) {
  ST_SCHEMA_TABLE *schema_table = static_cast<ST_SCHEMA_TABLE *>(ptr);
  schema_table->fields_info = Show::wsrep_cert_keys_view_fields;
  schema_table->fill_table = wsrep_cert_keys_fill_view;
  wsrep_cert_key_history_init();
  return 0;
}

static int wsrep_cert_keys_deinit(void *) {
  wsrep_cert_key_history_deinit();
  return 0;
}

/*
   WSREP_CERT_KEYS_HISTORY
*/
static struct st_mysql_information_schema wsrep_cert_keys_history_view = {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};

enum cert_keys_history_columns {
  CERT_KEYS_HISTORY_COLUMN_THD_ID = 0,
  CERT_KEYS_HISTORY_COLUMN_KEY_STRING,
  CERT_KEYS_HISTORY_COLUMN_KEY_TYPE
};

namespace Show {
static ST_FIELD_INFO wsrep_cert_keys_history_view_fields[] =
{
  Column("THD_ID", ULonglong(20), NOT_NULL, "Thread ID"),
  Column("KEY_STRING", Varchar(256), NOT_NULL, "Certification Key"),
  Column("KEY_TYPE", Varchar(64), NOT_NULL, "Certification Key Type"),
  CEnd()
};
} // namespace Show

struct wsrep_cert_keys_history_fill_func {
  void operator()(THD *thd) {
    if (!WSREP(thd)) {
      return;
    }
    struct call_ctx {
      THD *thd;
      TABLE *table;
    };
    call_ctx ctx = {thd, m_table};
    wsrep_cert_key_history_foreach_history(
        [](void *ctx, my_thread_id thd_id, const char *key_string,
           Wsrep_service_key_type key_type) {
          call_ctx *c = (call_ctx *)ctx;
          c->table->field[CERT_KEYS_HISTORY_COLUMN_THD_ID]->store(thd_id, true);
          c->table->field[CERT_KEYS_HISTORY_COLUMN_KEY_STRING]->store(
              key_string, strlen(key_string), system_charset_info);
          c->table->field[CERT_KEYS_HISTORY_COLUMN_KEY_TYPE]->store(
              wsrep_service_key_type_str(key_type),
              strlen(wsrep_service_key_type_str(key_type)),
              system_charset_info);
          schema_table_store_record(c->thd, c->table);
          return true;
        },
        &ctx);
  }

  wsrep_cert_keys_history_fill_func(THD *caller_thd, TABLE *table)
      : m_caller_thd(caller_thd), m_table(table) {}

  THD *m_caller_thd;
  TABLE *m_table;
};

static int wsrep_cert_keys_history_fill_view(THD *thd, TABLE_LIST *tables,
                                             Item *) {
  TABLE *table = tables->table;
  wsrep_cert_keys_history_fill_func f(thd, table);
  f(thd);
  return 0;
}

static int wsrep_cert_keys_history_init(void *ptr) {
  ST_SCHEMA_TABLE *schema_table = static_cast<ST_SCHEMA_TABLE *>(ptr);
  schema_table->fields_info = Show::wsrep_cert_keys_history_view_fields;
  schema_table->fill_table = wsrep_cert_keys_history_fill_view;
  wsrep_cert_key_history_init();
  return 0;
}

static int wsrep_cert_keys_history_deinit(void *) {
  wsrep_cert_key_history_deinit();
  return 0;
}

maria_declare_plugin(wsrep_info)
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &wsrep_memb_plugin,
  "WSREP_MEMBERSHIP",                           /* Plugin name        */
  "Nirbhay Choubey",                            /* Plugin author      */
  "Information about group members",            /* Plugin description */
  PLUGIN_LICENSE_GPL,                           /* License            */
  wsrep_memb_plugin_init,                       /* Plugin Init        */
  0,                                            /* Plugin Deinit      */
  0x0100,                                       /* Version (hex)      */
  NULL,                                         /* Status variables   */
  NULL,                                         /* System variables   */
  "1.0",                                        /* Version (string)   */
  MariaDB_PLUGIN_MATURITY_STABLE                /* Maturity           */
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &wsrep_status_plugin,
  "WSREP_STATUS",                               /* Plugin name        */
  "Nirbhay Choubey",                            /* Plugin author      */
  "Group view information",                     /* Plugin description */
  PLUGIN_LICENSE_GPL,                           /* License            */
  wsrep_status_plugin_init,                     /* Plugin Init        */
  0,                                            /* Plugin Deinit      */
  0x0100,                                       /* Version (hex)      */
  NULL,                                         /* Status variables   */
  NULL,                                         /* System variables   */
  "1.0",                                        /* Version (string)   */
  MariaDB_PLUGIN_MATURITY_STABLE                /* Maturity           */
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &wsrep_thd_info_history_plugin,
  "WSREP_THD_INFO_HISTORY",                     /* Plugin name        */
  "Codership Oy",                               /* Plugin author      */
  "Provides information about Galera thread state history",
  PLUGIN_LICENSE_GPL,                           /* License            */
  wsrep_thd_info_history_plugin_init,           /* Plugin Init        */
  wsrep_thd_info_history_plugin_deinit,         /* Plugin Deinit      */
  0x0100,                                       /* Version (hex)      */
  nullptr,                                      /* Status variables   */
  nullptr,                                      /* System variables   */
  PACKAGE_VERSION,                              /* Version (string)   */
  MariaDB_PLUGIN_MATURITY_STABLE                /* Maturity           */
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &wsrep_bf_abort_history_plugin,
  "WSREP_BF_ABORT_HISTORY",                     /* Plugin name        */
  "Codership Oy",                               /* Plugin author      */
  "Provides information about Galera bf abort history",
  PLUGIN_LICENSE_GPL,                           /* License            */
  wsrep_bf_abort_history_plugin_init,           /* Plugin Init        */
  wsrep_bf_abort_history_plugin_deinit,         /* Plugin Deinit      */
  0x0100,                                       /* Version (hex)      */
  nullptr,                                      /* Status variables   */
  nullptr,                                      /* System variables   */
  PACKAGE_VERSION,                              /* Version (string)   */
  MariaDB_PLUGIN_MATURITY_STABLE                /* Maturity           */
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &wsrep_cert_keys_view,
  "WSREP_CERT_KEYS",                            /* Plugin name        */
  "Codership Oy",                               /* Plugin author      */
  "Wsrep certification keys information",       /* Plugin description */
  PLUGIN_LICENSE_GPL,                           /* License            */
  wsrep_cert_keys_init,                         /* Plugin Init        */
  wsrep_cert_keys_deinit,                       /* Plugin Deinit      */
  0x0100,                                       /* Version (hex)      */
  NULL,                                         /* Status variables   */
  NULL,                                         /* System variables   */
  "1.0",                                        /* Version (string)   */
  MariaDB_PLUGIN_MATURITY_STABLE                /* Maturity           */
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &wsrep_cert_keys_history_view,
  "WSREP_CERT_KEYS_HISTORY",                      /* Plugin name        */
  "Codership Oy",                                 /* Plugin author      */
  "Wsrep certification keys history information", /* Plugin description */
  PLUGIN_LICENSE_GPL,                             /* License            */
  wsrep_cert_keys_history_init,                   /* Plugin Init        */
  wsrep_cert_keys_history_deinit,                 /* Plugin Deinit      */
  0x0100,                                         /* Version (hex)      */
  NULL,                                           /* Status variables   */
  NULL,                                           /* System variables   */
  "1.0",                                          /* Version (string)   */
  MariaDB_PLUGIN_MATURITY_STABLE                  /* Maturity           */
}
maria_declare_plugin_end;

