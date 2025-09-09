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

/** @file wsrep_thd_info_history.h

  This file provides declaratios for wsrep_thd_info_history
  plugin implementation.
*/

#ifndef WSREP_THD_INFO_HISTORY_H
#define WSREP_THD_INFO_HISTORY_H

#include "my_global.h"
#include "my_sys.h"    // my_hrtime_t

typedef struct
{
  long long thread_id;
  long long os_thread_id;
  my_hrtime_t state_time;
  const char* cs_state;
  const char* cs_mode;
  const char* trx_state;
  long long trx_id;
  long long seq_no;
  long long depends_on;
  std::string gtid;
} wsrep_thd_info_history_t;

#define WSREP_THD_INFO_HISTORY_LEN 1024
#define WSREP_THD_INFO_HISTORY_CACHE 24

extern std::mutex wsrep_thd_info_mutex;

#endif /*  WSREP_THD_INFO_HISTORY_H */
