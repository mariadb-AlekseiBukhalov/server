/* Copyright 2025 Codership Oy <info@codership.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/** @file wsrep_bf_abort_history.h

  This file provides declaratios for wsrep_bf_abort_history
  plugin implementation.
*/

#ifndef WSREP_BF_ABORT_HISTORY_H
#define WSREP_BF_ABORT_HISTORY_H

#include "my_global.h"
#include "my_sys.h"    // my_hrtime_t
#include <string>

#define WSREP_BF_ABORT_HISTORY_LEN 1024

typedef struct wsrep_bf_abort
{
  uint32_t victim_trx_id;
  uint32_t victim_thread_id;
  uint64_t victim_query_id;
  uint64_t victim_seqno;
  const char* victim_client_state;
  const char* victim_client_mode;
  const char* victim_trx_state;
  std::string victim_lock;
  my_hrtime_t time;
  uint32_t bf_trx_id;
  uint32_t bf_thread_id;
  uint64_t bf_query_id;
  uint64_t bf_seqno;
  const char* bf_client_state;
  const char* bf_client_mode;
  const char* bf_trx_state;
  std::string bf_lock;
  uint32_t space_id;
  uint32_t page_no;
  std::string index_name;
  std::string table_name;
} wsrep_bf_abort_t;

/** Function pointer typedef for storing wsrep bf abort info
to history table for plugin */
typedef int (*wsrep_bf_abort_store_func_t)(THD*, const wsrep_bf_abort_t*);

/** Function pointer for storing wsrep bf abort info */
extern wsrep_bf_abort_store_func_t wsrep_bf_abort_store_func;

/** wsrep_bf_abort_history_plugin enabled ? */
extern my_bool wsrep_bf_abort_history_enabled;

#endif /*  WSREP_BF_ABORT_HISTORY_H */
