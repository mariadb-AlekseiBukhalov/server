/* Copyright (c) 2002, 2012, eperi GmbH.

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; version 2 of the License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */

#include <my_global.h>
#include <typelib.h>
#include "parser.h"
#include <mysql/plugin_encryption.h>
#include <string.h>
#include <sql_i_s.h>
#include "log.h"
#include "sql_acl.h"                // PROCESS_ACL

#define PLUGIN_VERSION 0x0200
#define PLUGIN_VERSION_STR "2.0"

namespace file_key_management_plugin
{


static char* filename;
static char* filekey;
static unsigned long encryption_algorithm;

static const char *encryption_algorithm_names[]=
{
  "aes_cbc",
#ifdef HAVE_EncryptAes128Ctr
  "aes_ctr",
#endif
  0
};

static TYPELIB encryption_algorithm_typelib=CREATE_TYPELIB_FOR(encryption_algorithm_names);

static MYSQL_SYSVAR_STR(filename, filename,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Path and name of the key file",
  NULL, NULL, "");

static MYSQL_SYSVAR_STR(filekey, filekey,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Key to encrypt / decrypt the keyfile",
  NULL, NULL, "");

#ifdef HAVE_EncryptAes128Ctr
#define recommendation  ", aes_ctr is the recommended one"
#else
#define recommendation  ""
#endif
static MYSQL_SYSVAR_ENUM(encryption_algorithm, encryption_algorithm,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Encryption algorithm to use" recommendation,
  NULL, NULL, 0, &encryption_algorithm_typelib);

static struct st_mysql_sys_var* settings[] = {
  MYSQL_SYSVAR(filename),
  MYSQL_SYSVAR(filekey),
  MYSQL_SYSVAR(encryption_algorithm),
  NULL
};

/*
  The Keymap is protected with a read-write lock to prevent data races.
  Read operations (lookups) use a shared lock for high concurrency, while
  the write operation (swapping the map on reload) uses an exclusive lock.

  Benchmarks indicate no noticeable difference and should this lock ever become
  a bottleneck, this can be replaced with lock-free implementations.
*/
Keymap keys;
static bool loaded= false; // flag to check if the main plugin is loaded
mysql_rwlock_t lock;

static void register_and_init_lock()
{
#ifdef HAVE_PSI_INTERFACE
  static PSI_rwlock_key key_rwlock_LOCK_keymap;
  static PSI_rwlock_info file_key_managment_plugin_rwlocks[]=
  {
    { &key_rwlock_LOCK_keymap, "LOCK_keymap", 0}
  };
  int count= array_elements(file_key_managment_plugin_rwlocks);
  mysql_rwlock_register("plugin", file_key_managment_plugin_rwlocks, count);
#endif

  mysql_rwlock_init(key_rwlock_LOCK_keymap, &lock);
}

static unsigned int get_latest_version(uint key_id)
{
  mysql_rwlock_rdlock(&lock);
  auto it= keys.find(key_id);
  if (it == keys.end())
  {
    mysql_rwlock_unlock(&lock);
    return ENCRYPTION_KEY_VERSION_INVALID;
  }

  auto &versions= it->second;
  if (versions.empty())
  {
    mysql_rwlock_unlock(&lock);
    return ENCRYPTION_KEY_VERSION_INVALID;
  }
  unsigned int version= versions.rbegin()->first;
  mysql_rwlock_unlock(&lock);
  return version;
}

static unsigned int get_key_from_key_file(unsigned int key_id,
       unsigned int key_version, unsigned char* dstbuf, unsigned *buflen)
{
  mysql_rwlock_rdlock(&lock);
  auto it= keys.find(key_id);
  if (it == keys.end())
  {
    mysql_rwlock_unlock(&lock);
    return ENCRYPTION_KEY_VERSION_INVALID;
  }

  auto &versions= it->second;
  auto it_v= versions.find(key_version);
  if (it_v == versions.end())
  {
    mysql_rwlock_unlock(&lock);
    return ENCRYPTION_KEY_VERSION_INVALID;
  }

  keyentry *entry= &it_v->second;
  if (*buflen < entry->length)
  {
    *buflen= entry->length;
    mysql_rwlock_unlock(&lock);
    return ENCRYPTION_KEY_BUFFER_TOO_SMALL;
  }

  *buflen= entry->length;
  if (dstbuf)
    memcpy(dstbuf, entry->key, entry->length);

  mysql_rwlock_unlock(&lock);
  return 0;
}

// let's simplify the condition below
#ifndef HAVE_EncryptAes128Gcm
#define MY_AES_GCM MY_AES_CTR
#ifndef HAVE_EncryptAes128Ctr
#define MY_AES_CTR MY_AES_CBC
#endif
#endif

static inline enum my_aes_mode mode(int flags)
{
  /*
    If encryption_algorithm is AES_CTR then
      if no-padding, use AES_CTR
      else use AES_GCM (like CTR but appends a "checksum" block)
    else
      use AES_CBC
  */
  if (encryption_algorithm)
    if (flags & ENCRYPTION_FLAG_NOPAD)
      return MY_AES_CTR;
    else
      return MY_AES_GCM;
  else
    return MY_AES_CBC;
}

static int ctx_init(void *ctx, const unsigned char* key, unsigned int klen,
                    const unsigned char* iv, unsigned int ivlen, int flags,
                    unsigned int key_id, unsigned int key_version)
{
  return my_aes_crypt_init(ctx, mode(flags), flags, key, klen, iv, ivlen);
}

static int ctx_update(void *ctx, const unsigned char *src, unsigned int slen,
  unsigned char *dst, unsigned int *dlen)
{
  return my_aes_crypt_update(ctx, src, slen, dst, dlen);
}


static int ctx_finish(void *ctx, unsigned char *dst, unsigned int *dlen)
{
  return my_aes_crypt_finish(ctx, dst, dlen);
}

static unsigned int get_length(unsigned int slen, unsigned int key_id,
                               unsigned int key_version)
{
  return my_aes_get_size(mode(0), slen);
}

static uint ctx_size(uint, uint)
{
  return my_aes_ctx_size(mode(0));
}

struct st_mariadb_encryption file_key_management_plugin= {
  MariaDB_ENCRYPTION_INTERFACE_VERSION,
  get_latest_version,
  get_key_from_key_file,
  ctx_size,
  ctx_init,
  ctx_update,
  ctx_finish,
  get_length
};

static int reload_keys()
{
  Keymap temp_keys;
  Parser parser(filename, filekey);

  if (!parser.parse(&temp_keys))
  {
    mysql_rwlock_wrlock(&lock);
    keys.swap(temp_keys);
    mysql_rwlock_unlock(&lock);
    return 0;
  }
  return 1;
}

static int file_key_management_plugin_init(void *p)
{
  register_and_init_lock();
  if (reload_keys() == 0)
  {
    loaded= true;
    return 0; // success
  }
  return 1; // failed
}

static int file_key_management_plugin_deinit(void *p)
{
  keys.clear();
  mysql_rwlock_destroy(&lock);
  return 0;
}

static struct st_mysql_information_schema reload_keys_info=
                              { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION };

static ST_FIELD_INFO file_key_management_reload_keys[]=
{
  Show::Column("KEY_ID",      Show::ULonglong(32), NOT_NULL, "KEY_ID"),
  Show::Column("KEY_VERSION", Show::ULonglong(32), NOT_NULL, "KEY_VERSION"),
  Show::CEnd()
};


/*
 @brief Fill FILE_KEY_MANAGEMENT_KEYS I_S table with key id & latest key version

 1 - Error / Error when filling I_S table
 0 - Success
*/
static int fill_filekeys_table(THD *thd, TABLE_LIST *tables, COND *cond)
{
  TABLE *table= tables->table;
  bool is_show= thd_sql_command(thd) == SQLCOM_SHOW_GENERIC;

  if (check_global_access(thd, PROCESS_ACL, !is_show))
    return is_show;

  mysql_rwlock_rdlock(&lock);
  for (auto it= keys.begin(); it != keys.end(); it++)
  {
    table->field[0]->store(it->first, 0);

    auto &versions= it->second;
    if (versions.empty())
    {
      mysql_rwlock_unlock(&lock);
      return 1;
    }

    table->field[1]->store(versions.rbegin()->first, 0);

    if (schema_table_store_record(thd, table))
    {
      mysql_rwlock_unlock(&lock);
      return 1;
    }
  }

  mysql_rwlock_unlock(&lock);
  return 0;
}

/**
 @breif Reload keys after key rotation

 1 - Error  / Could not read new version of the key
 0 - Succes / Successfully read new keyfile
 */
static int reload_keys_callback()
{
  int err= reload_keys();
  if (err)
    sql_print_information("file_key_management: Failed to reload keys");
  else
    sql_print_information("file_key_management: Reloading keys successful");
  return err;
}

static int reload_keys_init(void *p)
{
  if (!loaded)
    return 1; // failed

  ST_SCHEMA_TABLE *key_schema_table= (ST_SCHEMA_TABLE*)p;
  key_schema_table->fields_info= file_key_management_reload_keys;
  key_schema_table->fill_table= fill_filekeys_table;
  key_schema_table->hidden= false;
  key_schema_table->reset_table= reload_keys_callback;

  return 0; // success
}

} // namespace file_key_management_plugin

/*
  Plugin library descriptor
*/
maria_declare_plugin(file_key_management)
{
  MariaDB_ENCRYPTION_PLUGIN,
  &file_key_management_plugin::file_key_management_plugin,
  "file_key_management",
  "Denis Endro eperi GmbH",
  "File-based key management plugin",
  PLUGIN_LICENSE_GPL,
  file_key_management_plugin::file_key_management_plugin_init,
  file_key_management_plugin::file_key_management_plugin_deinit,
  PLUGIN_VERSION /* 2.0 */,
  NULL,	/* status variables */
  file_key_management_plugin::settings,
  PLUGIN_VERSION_STR,
  MariaDB_PLUGIN_MATURITY_GAMMA
},
{
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &file_key_management_plugin::reload_keys_info,
  "file_key_management_keys",
  "Raghunandan Bhat",
  "Reload encryption keys after key rotation",
  PLUGIN_LICENSE_GPL,
  file_key_management_plugin::reload_keys_init,
  NULL,
  PLUGIN_VERSION,
  NULL,
  NULL,
  PLUGIN_VERSION_STR,
  MariaDB_PLUGIN_MATURITY_GAMMA
}
maria_declare_plugin_end;
