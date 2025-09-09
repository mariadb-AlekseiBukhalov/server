/* Copyright 2024-2025 Codership Oy <http://www.codership.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02111-1301 USA */

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
#include "wsrep_schema.h"
#include <stdint.h>
#include <map>
#include <mutex>
#include "wsrep_connection_monitor_service.h"
#include "wsrep_i_s_helper.h"

/** Mutex protecting in-memory cache of the wsrep connections */
static std::mutex wsrep_connections_mutex;

/** Wsrep connections in-memory cache */
static std::map<uintptr_t, wsrep_connection_t> wsrep_connections;

static int wsrep_connection_monitor_plugin_deinit(void *p)
{
  return 0;
}

namespace Show {
static ST_FIELD_INFO wsrep_connections_fields_info[]=
{
#define CONNECTION_ID 0
  Column ("connection_id", ULonglong(), NOT_NULL),
#define CONNECTION_SCHEME 1
  Column ("connection_scheme", Varchar(WSREP_CMP_SCHEME_LEN), NOT_NULL),
#define LOCAL_ADDRESS 2
  Column ("local_address", Varchar(WSREP_CMP_BIG_LEN), NOT_NULL),
#define REMOTE_ADDRESS 3
  Column ("remote_address", Varchar(WSREP_CMP_BIG_LEN), NOT_NULL),
#define CIPHER 4
  Column ("cipher", Varchar(WSREP_CMP_BIG_LEN), NOT_NULL),
#define CERTIFICATE_SUBJECT 5
  Column ("certificate_subject", Varchar(WSREP_CMP_BIG_LEN), NOT_NULL),
#define CERTIFICATE_ISSUER 6
  Column ("certificate_issuer", Varchar(WSREP_CMP_BIG_LEN), NOT_NULL),
#define CERTIFICATE_VERSION 7
  Column ("certificate_version", Varchar(WSREP_CMP_BIG_LEN), NOT_NULL),

  CEnd()
};
} // namespace Show

static int fill_wsrep_connections(THD *thd, TABLE_LIST *tables, Item *)
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
  std::lock_guard<std::mutex> wsrep_connections_lock(wsrep_connections_mutex);
  for (const auto &c : wsrep_connections)
  {
    const wsrep_connection_t &conn = c.second;
    WSREP_IS_OK(store_longlong(fields[CONNECTION_ID], conn.connection_id));
    WSREP_IS_OK(store_string(fields[CONNECTION_SCHEME], conn.scheme));
    WSREP_IS_OK(store_string(fields[LOCAL_ADDRESS], conn.local_address));
    WSREP_IS_OK(store_string(fields[REMOTE_ADDRESS], conn.remote_address));
    WSREP_IS_OK(store_string(fields[CIPHER], conn.cipher));
    WSREP_IS_OK(store_string(fields[CERTIFICATE_SUBJECT], conn.certificate_subject));
    WSREP_IS_OK(store_string(fields[CERTIFICATE_ISSUER], conn.certificate_issuer));
    WSREP_IS_OK(store_string(fields[CERTIFICATE_VERSION], conn.version));
    WSREP_IS_OK(schema_table_store_record(thd, tables->table));
  }

  return 0;
}

/** Bind the dynamic table INFORMATION_SCHEMA.wsrep_connections
@return 0 on success */
static int wsrep_connection_monitor_plugin_init(void *p)
{
  ST_SCHEMA_TABLE* schema;

  schema = (ST_SCHEMA_TABLE*) p;
  schema->fields_info= Show::wsrep_connections_fields_info;
  schema->fill_table= fill_wsrep_connections;

  return 0;
}

static struct st_mysql_information_schema plugin_descriptor =
{
  MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION
};

maria_declare_plugin(wsrep_connection_monitor)
{
  /* the plugin type (a MYSQL_XXX_PLUGIN value) */
  /* int */
  MYSQL_INFORMATION_SCHEMA_PLUGIN,

  /* pointer to type-specific plugin descriptor */
  /* void* */
  &plugin_descriptor,

  /* plugin name */
  /* const char* */
  "wsrep_connections",

  /* plugin author (for SHOW PLUGINS) */
  /* const char* */
  "Codership Oy",

  /* general descriptive text (for SHOW PLUGINS) */
  /* const char* */
  "Provides information about Galera connections",

  /* the plugin license (PLUGIN_LICENSE_XXX) */
  /* int */
  PLUGIN_LICENSE_GPL,

  /* the function to invoke when plugin is loaded */
  /* int (*)(void*); */
  wsrep_connection_monitor_plugin_init,

  /* the function to invoke when plugin is unloaded */
  /* int (*)(void*); */
  wsrep_connection_monitor_plugin_deinit,

  0x0100, nullptr, nullptr, PACKAGE_VERSION,
  MariaDB_PLUGIN_MATURITY_STABLE
}
maria_declare_plugin_end;

bool wsrep_connection_monitor_connect(wsrep_connection_key_t id,
                                      const std::string &scheme,
                                      const std::string &local_addr,
                                      const std::string &remote_addr)
{
  uintptr_t key= (uintptr_t)id;

  try
  {
    std::lock_guard<std::mutex> wsrep_lock(wsrep_connections_mutex);
    std::map<uintptr_t, wsrep_connection_t>::iterator i= wsrep_connections.find(key);

    // Not found : add
    if (i == wsrep_connections.end())
    {
      WSREP_DEBUG("wsrep_connection_add: key %ld not found add %s %s %s", key,
                  scheme.c_str(), local_addr.c_str(), remote_addr.c_str());
      wsrep_connection_t new_connection {key, scheme, local_addr, remote_addr,
                                         "","","",""};
      wsrep_connections.insert(std::pair<uintptr_t, wsrep_connection_t>(key, new_connection));
    }
    else
    {
      // found, update
      wsrep_connection_t &old_conn= i->second;
      WSREP_DEBUG("wsrep_connection_add: key %ld found %s %s %s new %s %s ",
                  key, old_conn.scheme.c_str(), old_conn.local_address.c_str(),
                  old_conn.remote_address.c_str(),
                  remote_addr.c_str(), local_addr.c_str());

      old_conn.scheme= scheme;
      old_conn.remote_address= remote_addr;
      old_conn.local_address= local_addr;
    }
  }
  catch(...)
  {
    WSREP_DEBUG("Adding connection monitoring information failied");
    return false;
  }

  return true;
}

bool wsrep_connection_monitor_disconnect(wsrep_connection_key_t id)
{
  uintptr_t key= (uintptr_t)id;

  try
  {
    std::lock_guard<std::mutex> wsrep_lock(wsrep_connections_mutex);
    std::map<uintptr_t, wsrep_connection_t>::iterator c= wsrep_connections.find(key);

    if (c != wsrep_connections.end())
    {
      wsrep_connection_t conn= c->second;
      WSREP_DEBUG("wsrep_connection_monitor_disconnect: found for %ld : %s %s %s",
                  key,
                  conn.scheme.c_str(), conn.local_address.c_str(),
                  conn.remote_address.c_str());
      wsrep_connections.erase(c);
    }
    else
      WSREP_DEBUG("wsrep_connection_monitor_disconnect: not found %ld", key);
  }
  catch (...)
  {
    WSREP_DEBUG("Removing connection information failed.");
    return false;
  }

  return true;
}

bool wsrep_connection_monitor_ssl_info(wsrep_connection_key_t id,
                                       const std::string &cipher,
                                       const std::string &certificate_subject,
                                       const std::string &certificate_issuer,
                                       const std::string &version)
{
  try
  {
    uintptr_t key= (uintptr_t)id;
    std::lock_guard<std::mutex> wsrep_lock(wsrep_connections_mutex);
    std::map<uintptr_t, wsrep_connection_t>::iterator i= wsrep_connections.find(key);

    if (i != wsrep_connections.end())
    {
      // found, update
      wsrep_connection_t &old_conn= i->second;
      WSREP_DEBUG("wsrep_connection_ssl_info: key %ld %s %s %s : %s %s %s %s",
                  key, old_conn.scheme.c_str(), old_conn.local_address.c_str(),
                  old_conn.remote_address.c_str(),
                  cipher.c_str(), certificate_subject.c_str(),
                  certificate_issuer.c_str(), version.c_str());
      old_conn.cipher= cipher;
      old_conn.certificate_subject= certificate_subject;
      old_conn.certificate_issuer= certificate_issuer;
      old_conn.version= version;
    }
    else
    {
      WSREP_DEBUG("wsrep_connection_monitor_ssl_info: not found %ld", key);
    }
  }
  catch (...)
  {
    WSREP_DEBUG("Adding ssl information failed");
    return false;
  }

  return true;
}
