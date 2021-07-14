/* Copyright (c) 2021, MariaDB Corporation.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

/**
  @file storage/perfschema/table_galera_group_members.cc
  Table galera_group_members (implementation).
*/

#include "my_global.h"
#include "table_galera_group_members.h"
#include "pfs_instr_class.h"
#include "pfs_instr.h"
#include "log.h"

#include <atomic>

THR_LOCK table_galera_group_members::m_table_lock;

PFS_engine_table_share_state
table_galera_group_members::m_share_state = {
  false /* m_checked */
};

#define STRINGIFY2(X) #X
#define STRINGIFY(X) STRINGIFY2(X)

PFS_engine_table_share
table_galera_group_members::m_share=
{
  { C_STRING_WITH_LEN("galera_group_members") },
  &pfs_readonly_acl,
  &table_galera_group_members::create,
  NULL, /* write_row */
  NULL, /* delete_all_rows */
  table_galera_group_members::get_row_count,
  sizeof(PFS_simple_index), /* ref length */
  &m_table_lock,
  { C_STRING_WITH_LEN("CREATE TABLE galera_group_members("
                      "wsrep_node_id char(" STRINGIFY(WSREP_UUID_STR_LEN) ") not null,"
                      "wsrep_cluster_state_uuid char(" STRINGIFY(WSREP_UUID_STR_LEN) ") not null,"
                      "wsrep_local_state_uuid char(" STRINGIFY(WSREP_UUID_STR_LEN) ") not null,"
                      "wsrep_last_applied BIGINT not null,"
                      "wsrep_last_committed BIGINT not null,"
                      "wsrep_replicated BIGINT not null,"
                      "wsrep_replicated_bytes BIGINT not null,"
                      "wsrep_received BIGINT not null,"
                      "wsrep_received_bytes BIGINT not null,"
                      "wsrep_local_bf_aborts BIGINT not null,"
                      "wsrep_local_commits BIGINT not null,"
                      "wsrep_local_cert_failures BIGINT not null,"
                      "wsrep_apply_window BIGINT not null,"
                      "wsrep_commit_window BIGINT not null)") },
  false, /* m_perpetual */
  false, /* m_optional */
  &m_share_state
};

PFS_engine_table* table_galera_group_members::create(void)
{
  return new table_galera_group_members();
}

static std::atomic<wsrep_ps_fetch_cluster_info_t> fetch_cluster_info;

static wsrep_ps_fetch_cluster_info_t init_once()
{
  if (auto f= fetch_cluster_info.load(std::memory_order_acquire))
    return f;

  union
  {
    void* sym;
    wsrep_ps_fetch_cluster_info_t func;
  } obj;
  obj.sym= dlsym(
    ((wsrep_t*)Wsrep_server_state::instance().get_provider().native())->dlh,
    WSREP_PS_FETCH_CLUSTER_INFO_FUNC);
  if (obj.func)
    fetch_cluster_info.store(obj.func, std::memory_order_release);
  DBUG_PRINT("info", ("Initialized galera PS fetch cluster info: %p %p",
                      ((wsrep_t*)Wsrep_server_state::instance().
                       get_provider().native())->dlh,
                      obj.sym));
  return obj.func;
}

table_galera_group_members::table_galera_group_members()
  : PFS_engine_table(&m_share, &m_pos),
    m_row_exists(false), m_pos(0), m_next_pos(0)
{}

table_galera_group_members::~table_galera_group_members()
{}

void table_galera_group_members::reset_position(void)
{
  m_pos.m_index= 0;
  m_next_pos.m_index= 0;
}

ha_rows table_galera_group_members::get_row_count(void)
{
  if (WSREP_ON)
  {
    if (!init_once())
      return 0;
    ha_rows total= wsrep_cluster_size;
    if (total > PFS_GALERA_MAX_CLUSTER_SIZE)
    {
      total= PFS_GALERA_MAX_CLUSTER_SIZE;
    }
    return total;
  }
  return 0;
}

int table_galera_group_members::rnd_init(bool) {
  m_rows= 0;
  if (!WSREP_ON)
    return 0;
  auto fetch= init_once();
  if (!fetch)
    return 0;
  uint32_t size= PFS_GALERA_MAX_CLUSTER_SIZE;
  wsrep_status_t ret=
    fetch((wsrep_t*)Wsrep_server_state::instance().get_provider().native(),
          m_entries, &size);
  if (ret == WSREP_OK)
  {
    m_rows= size;
  }
  return 0;
}

int table_galera_group_members::rnd_next(void)
{
  if (!WSREP_ON)
    return HA_ERR_END_OF_FILE;

  for (m_pos.set_at(&m_next_pos);
       m_pos.m_index < get_row_count();
       m_pos.next())
  {
    make_row(m_pos.m_index);
    m_next_pos.set_after(&m_pos);
    return 0;
  }

  return HA_ERR_END_OF_FILE;
}

int table_galera_group_members::rnd_pos(const void *pos)
{
  if (!WSREP_ON)
    return HA_ERR_END_OF_FILE;

  set_position(pos);
  DBUG_ASSERT(m_pos.m_index < get_row_count());
  make_row(m_pos.m_index);

  return 0;
}

void table_galera_group_members::make_row(uint index)
{
  DBUG_ENTER("table_galera_group_members::make_row");
  // Set default values.
  m_row_exists= false;

  if (index >= m_rows) {
    DBUG_ASSERT(false);
  }

  // Query plugin and let callbacks do their job.
  if (WSREP_ON)
  {
    m_row_exists= true;
    m_row= m_entries[index];
  }
  else
  {
    DBUG_PRINT("info", ("Galera stats not available!"));
  }

  DBUG_VOID_RETURN;
}

int table_galera_group_members::read_row_values(TABLE *table,
                                                     unsigned char *buf,
                                                     Field **fields,
                                                     bool read_all)
{
  Field *f;

  if (unlikely(! m_row_exists))
    return HA_ERR_RECORD_DELETED;

  DBUG_ASSERT(table->s->null_bytes == 0);
  buf[0]= 0;

  for (; (f= *fields) ; fields++)
  {
    if (read_all || bitmap_is_set(table->read_set, f->field_index))
    {
      switch(f->field_index)
      {
      case 0: /** wsrep_node_id */
        set_field_char_utf8(f, m_row.wsrep_node_id,
                            WSREP_UUID_STR_LEN);
        break;
      case 1: /** wsrep_cluster_state_uuid */
        set_field_char_utf8(f, m_row.wsrep_cluster_state_uuid,
                            WSREP_UUID_STR_LEN);
        break;
      case 2: /** wsrep_local_state_uuid */
        set_field_char_utf8(f, m_row.wsrep_local_state_uuid,
                            WSREP_UUID_STR_LEN);
        break;
      case 3: /** wsrep_last_applied */
        set_field_ulong(f, m_row.wsrep_last_applied);
        break;
      case 4: /** wsrep_last_committed */
        set_field_ulong(f, m_row.wsrep_last_committed);
        break;
      case 5: /** wsrep_replicated */
        set_field_ulong(f, m_row.wsrep_replicated);
        break;
      case 6: /** wsrep_replicated_bytes */
        set_field_ulong(f, m_row.wsrep_replicated_bytes);
        break;
      case 7: /** wsrep_received */
        set_field_ulong(f, m_row.wsrep_received);
        break;
      case 8: /** wsrep_received_bytes */
        set_field_ulong(f, m_row.wsrep_received_bytes);
        break;
      case 9: /** wsrep_local_bf_aborts */
        set_field_ulong(f, m_row.wsrep_local_bf_aborts);
        break;
      case 10: /** wsrep_local_commits */
        set_field_ulong(f, m_row.wsrep_local_commits);
        break;
      case 11: /** wsrep_local_cert_failures */
        set_field_ulong(f, m_row.wsrep_local_cert_failures);
        break;
      case 12: /** wsrep_apply_window */
        set_field_ulong(f, m_row.wsrep_apply_window);
        break;
      case 13: /** wsrep_commit_window */
        set_field_ulong(f, m_row.wsrep_commit_window);
        break;
      default:
        DBUG_ASSERT(false);
      }
    }
  }
  return 0;
}
