/* Copyright 2018-2025 Codership Oy <info@codership.com>

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

#ifndef WSREP_CLIENT_STATE_H
#define WSREP_CLIENT_STATE_H

/* wsrep-lib */
#include "wsrep/client_state.hpp"
#include "my_global.h"
#include "wsrep_thd_info_history.h"
#include <deque>

class THD;

class Wsrep_client_state : public wsrep::client_state
{
public:
  Wsrep_client_state(THD* thd,
                     wsrep::mutex& mutex,
                     wsrep::condition_variable& cond,
                     wsrep::server_state& server_state,
                     wsrep::client_service& client_service,
                     const wsrep::client_id& id)
    : wsrep::client_state(mutex,
                          cond,
                          server_state,
                          client_service,
                          id,
                          wsrep::client_state::m_local)
    , m_thd(thd)
  { }
  ~Wsrep_client_state();
  THD* thd() { return m_thd; }

  /* Function to return size of thd history */
  size_t thd_history_size() const { return m_thd_history.size(); }
  /* Function to empty thd history deque */
  void thd_history_clear() { m_thd_history.clear(); }
  /* Function to return reference to thd history deque */
  const std::deque<wsrep_thd_info_history_t>& thd_history() const { return m_thd_history; }
  int thd_history_push(const wsrep_thd_info_history_t &thd_hist);

private:
  THD* m_thd;
  std::deque<wsrep_thd_info_history_t> m_thd_history;
};

#endif /* WSREP_CLIENT_STATE_H */
