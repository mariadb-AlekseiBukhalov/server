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

#include "wsrep/client_state.hpp"
#include "my_global.h"
#include "sql_class.h"
#include "wsrep_client_state.h"
#include "wsrep_thd_info_history.h"
#include <deque>
#include "wsrep_mysqld.h"
#include <mutex>

/** Mutex protecting in-memory cache of the wsrep thd info history */
std::mutex wsrep_thd_info_mutex;

int Wsrep_client_state::thd_history_push(const wsrep_thd_info_history_t &thd_hist)
{
  std::lock_guard<std::mutex> wsrep_thd_mutex(wsrep_thd_info_mutex);
  try
  {
    m_thd_history.push_back(thd_hist);

    if (m_thd_history.size() > WSREP_THD_INFO_HISTORY_CACHE)
	m_thd_history.pop_front();
  }
  catch(...)
  {
    WSREP_DEBUG("Storing wsrep thd info to history cache failed.");
    return 1;
  }

  return 0;
}

Wsrep_client_state::~Wsrep_client_state()
{
  /* Aggregate stored thd info history if exists */
  std::lock_guard<std::mutex> wsrep_thd_mutex(wsrep_thd_info_mutex);
  if (wsrep_thd_info_aggregate_func && m_thd_history.size())
  {
    wsrep_thd_info_aggregate_func(m_thd, true);
    thd_history_clear();
  }
}
