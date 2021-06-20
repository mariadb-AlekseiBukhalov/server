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
  @file storage/perfschema/table_galera_group_member_stats.cc
  Table galera_group_member_stats (implementation).
*/

#include "my_global.h"
#include "table_galera_group_member_stats.h"
#include "pfs_instr_class.h"
#include "pfs_instr.h"
#include "log.h"
#include "wsrep/ps_service.hpp"

THR_LOCK table_galera_group_member_stats::m_table_lock;

#define STRINGIFY2(X) #X
#define STRINGIFY(X) STRINGIFY2(X)

PFS_engine_table_share
table_galera_group_member_stats::m_share=
{
  { C_STRING_WITH_LEN("galera_group_member_stats") },
  &pfs_readonly_acl,
  &table_galera_group_member_stats::create,
  NULL, /* write_row */
  NULL, /* delete_all_rows */
  table_galera_group_member_stats::get_row_count,
  sizeof(PFS_simple_index), /* ref length */
  &m_table_lock,
  { C_STRING_WITH_LEN("CREATE TABLE galera_group_member_stats("
                      "wsrep_node_id char(" STRINGIFY(WSREP_UUID_STR_LEN) "),"
                      "wsrep_local_index BIGINT,"
                      "wsrep_repl_keys BIGINT,"
                      "wsrep_repl_keys_bytes BIGINT,"
                      "wsrep_repl_data_bytes BIGINT,"
                      "wsrep_repl_other_bytes BIGINT,"
                      "wsrep_local_replays BIGINT,"
                      "wsrep_local_send_queue BIGINT,"
                      "wsrep_local_send_queue_avg DOUBLE PRECISION,"
                      "wsrep_local_recv_queue BIGINT,"
                      "wsrep_local_recv_queue_avg DOUBLE PRECISION,"
                      "wsrep_flow_control_paused BIGINT,"
                      "wsrep_flow_control_sent BIGINT,"
                      "wsrep_flow_control_recv BIGINT,"
                      "wsrep_flow_control_status char("
                         STRINGIFY(WSREP_STATUS_LENGTH)
                      "),"
                      "wsrep_cert_deps_distance DOUBLE PRECISION,"
                      "wsrep_open_transactions BIGINT,"
                      "wsrep_evs_repl_latency BIGINT)") },
  false  /* perpetual */
};

PFS_engine_table* table_galera_group_member_stats::create(void)
{
  return new table_galera_group_member_stats();
}

table_galera_group_member_stats::table_galera_group_member_stats()
  : PFS_engine_table(&m_share, &m_pos),
    m_row_exists(false), m_pos(0), m_next_pos(0)
{}

table_galera_group_member_stats::~table_galera_group_member_stats()
{}

void table_galera_group_member_stats::reset_position(void)
{
  m_pos.m_index= 0;
  m_next_pos.m_index= 0;
}

ha_rows table_galera_group_member_stats::get_row_count()
{
  if (WSREP_ON)
  {
    if (wsrep_cluster_size > 0)
    {
      return 1;
    }
  }
  return 0;
}

int table_galera_group_member_stats::rnd_init(bool) {
  m_rows= 0;
  if (!WSREP_ON)
    return 0;
  wsrep_status_t ret=
    Wsrep_server_state::instance().provider_services().ps_service->
      fetch_pfs_stat.load(std::memory_order_relaxed)
        (&Wsrep_server_state::instance().get_provider(), m_entries);
  if (ret == WSREP_OK)
  {
    m_rows= 1;
  }
  return 0;
}

int table_galera_group_member_stats::rnd_next(void)
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

int table_galera_group_member_stats::rnd_pos(const void *pos)
{
  if (!WSREP_ON)
    return HA_ERR_END_OF_FILE;

  set_position(pos);
  DBUG_ASSERT(m_pos.m_index < get_row_count());
  make_row(m_pos.m_index);

  return 0;
}

void table_galera_group_member_stats::make_row(uint index)
{
  DBUG_ENTER("table_galera_group_member_stats::make_row");
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

int table_galera_group_member_stats::read_row_values(TABLE *table,
                                                   unsigned char *buf,
                                                   Field **fields,
                                                   bool read_all)
{
  Field *f;

  if (unlikely(! m_row_exists))
    return HA_ERR_RECORD_DELETED;

  DBUG_ASSERT(table->s->null_bytes == 1);
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
      case 1: /** wsrep_local_index */
        set_field_ulong(f, m_row.wsrep_local_index);
        break;
      case 2: /** wsrep_repl_keys */
        set_field_ulong(f, m_row.wsrep_repl_keys);
        break;
      case 3: /** wsrep_repl_keys_bytes */
        set_field_ulong(f, m_row.wsrep_repl_keys_bytes);
        break;
      case 4: /** wsrep_repl_data_bytes */
        set_field_ulong(f, m_row.wsrep_repl_data_bytes);
        break;
      case 5: /** wsrep_repl_other_bytes */
        set_field_ulong(f, m_row.wsrep_repl_other_bytes);
        break;
      case 6: /** wsrep_local_replays */
        set_field_ulong(f, m_row.wsrep_local_replays);
        break;
      case 7: /** wsrep_local_send_queue */
        set_field_ulong(f, m_row.wsrep_local_send_queue);
        break;
      case 8: /** wsrep_local_send_queue_avg */
        set_field_double(f, m_row.wsrep_local_send_queue_avg);
        break;
      case 9: /** wsrep_local_recv_queue */
        set_field_ulong(f, m_row.wsrep_local_recv_queue);
        break;
      case 10: /** wsrep_local_recv_queue_avg */
        set_field_double(f, m_row.wsrep_local_recv_queue_avg);
        break;
      case 11: /** wsrep_flow_control_paused */
        set_field_ulong(f, m_row.wsrep_flow_control_paused);
        break;
      case 12: /** wsrep_flow_control_sent */
        set_field_ulong(f, m_row.wsrep_flow_control_sent);
        break;
      case 13: /** wsrep_flow_control_recv */
        set_field_ulong(f, m_row.wsrep_flow_control_recv);
        break;
      case 14: /** wsrep_flow_control_status */
        set_field_char_utf8(f, m_row.wsrep_flow_control_status,
                            WSREP_STATUS_LENGTH);
        break;
      case 15: /** wsrep_cert_deps_distance */
        set_field_double(f, m_row.wsrep_cert_deps_distance);
        break;
      case 16: /** wsrep_open_transactions */
        set_field_ulong(f, m_row.wsrep_open_transactions);
        break;
      case 17: /** wsrep_evs_repl_latency */
        set_field_ulong(f, m_row.wsrep_evs_repl_latency);
        break;
      default:
        DBUG_ASSERT(false);
      }
    }
  }
  return 0;
}
