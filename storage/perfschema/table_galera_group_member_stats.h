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


#ifndef TABLE_GALERA_GROUP_MEMBER_STATS_H
#define TABLE_GALERA_GROUP_MEMBER_STATS_H

/**
  @file storage/perfschema/table_galera_group_member_stats.h
  Table galera_group_member_stats (declarations).
*/

#include "pfs_column_types.h"
#include "pfs_engine_table.h"
#include "mysql_com.h"

#include "wsrep_ps.h"
#include "wsrep_mysqld.h"
#include "wsrep_server_state.h"

/**
  @addtogroup Performance_schema_tables
  @{
*/

#define PFS_GALERA_MAX_CLUSTER_SIZE 128

/** Table PERFORMANCE_SCHEMA.GALERA_GROUP_MEMBER_STATS. */
class table_galera_group_member_stats: public PFS_engine_table
{
public:
  static PFS_engine_table_share_state m_share_state;
  /** Table share */
  static PFS_engine_table_share m_share;
  /** Table builder */
  static PFS_engine_table* create();

private:
  void make_row(uint index);
  /** Table share lock. */
  static THR_LOCK m_table_lock;
  /** Fields definition. */
  static TABLE_FIELD_DEF m_field_def;
  /** True if the current row exists. */
  bool m_row_exists;
  /** Current row. */
  wsrep_node_stat_t m_row;
  /** Number of rows. */
  uint32_t m_rows;
  /** Row cache. */
  wsrep_node_stat_t m_entries[PFS_GALERA_MAX_CLUSTER_SIZE];
  /** Current position. */
  PFS_simple_index m_pos;
  /** Next position. */
  PFS_simple_index m_next_pos;

protected:
  /**
    Read the current row values.
    @param table            Table handle
    @param buf              row buffer
    @param fields           Table fields
    @param read_all         true if all columns are read.
  */

  int read_row_values(TABLE *table,
                      unsigned char *buf,
                      Field **fields,
                      bool read_all) override;

  table_galera_group_member_stats();

public:
  ~table_galera_group_member_stats() override;

  static ha_rows get_row_count();

  int rnd_init(bool) override;
  int rnd_next() override;
  int rnd_pos(const void *pos) override;
  void reset_position(void) override;

};

/** @} */
#endif
