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

#include "wsrep_cert_key_history.h"

#include "sql_class.h"

#include <deque>
#include <mutex>
#include <sstream>
#include <iomanip>

#define MAX_CERT_HISTORY_SIZE 1024
std::atomic<size_t> Wsrep_cert_key_history::initialized_count{0};
std::deque<Wsrep_cert_key_history::Key_entry> Wsrep_cert_key_history::keys_history{};
std::mutex Wsrep_cert_key_history::mutex{};

void Wsrep_cert_key_history::append(my_thread_id thd_id,
                                    const wsrep::key &key) {
  std::vector<std::string> key_parts;
  for (size_t i= 0; i < key.size(); ++i) {
    try
    {
      std::stringstream ss;
      for (size_t j(0); j < key.key_parts()[i].size(); ++j)
      {
          ss << std::hex
             << std::uppercase
             << std::setfill('0')
             << std::setw(2)
             << static_cast<int>(
                 *(reinterpret_cast<const unsigned char*>(key.key_parts()[i].data()) + j));
      }
      key_parts.push_back(ss.str());
    }
    catch (...)
    {
      WSREP_ERROR("Error while adding Wsrep Certification key parts.");
      return;
    }
  }

  Key_entry key_entry{thd_id, key_parts, key.type()};
  try
  {
    keys_.push_back(key_entry);
  }
  catch (...)
  {
    WSREP_ERROR("Error while adding Wsrep Certification key.");
    return;
  }
  append_history(key_entry);
}

void Wsrep_cert_key_history::append_history(const Key_entry &key_entry) {
  std::lock_guard<std::mutex> lock{mutex};
  try
  {
    keys_history.push_back(key_entry);
  }
  catch (...)
  {
    WSREP_ERROR("Error while adding Wsrep Certification key.");
    return;
  }
  if (keys_history.size() > MAX_CERT_HISTORY_SIZE) {
    try
    {
      keys_history.pop_front();
    }
    catch (...)
    {
      WSREP_ERROR("Error while removing Wsrep Certification key.");
      return;
    }
  }
}

void Wsrep_cert_key_history::show_history(bool (*fn)(void *ctx, my_thread_id,
                                            const char *,
                                            Wsrep_service_key_type),
                                 void *ctx) {
  std::lock_guard<std::mutex> lock{mutex};
  for (const auto &key_entry : keys_history) {
    show_one_key(key_entry, fn, ctx);
  }
}

static Wsrep_service_key_type map_key_type(enum wsrep::key::type key_type) {
  switch (key_type) {
    case wsrep::key::shared:
      return WSREP_SERVICE_KEY_SHARED;
    case wsrep::key::reference:
      return WSREP_SERVICE_KEY_REFERENCE;
    case wsrep::key::update:
      return WSREP_SERVICE_KEY_UPDATE;
    case wsrep::key::exclusive:
      return WSREP_SERVICE_KEY_EXCLUSIVE;
  }
  assert(false);
  return WSREP_SERVICE_KEY_SHARED;
}

void Wsrep_cert_key_history::show_one_key(const Key_entry &key_entry,
                                          bool (*fn)(void *ctx, my_thread_id,
                                                     const char *,
                                                     Wsrep_service_key_type),
                                          void *ctx) {
  std::string hex_string;
  for (const auto &part : key_entry.key_parts) {
    hex_string += part;
    hex_string += " ";
  }
  fn(ctx, key_entry.thd_id, hex_string.c_str(),
     map_key_type(key_entry.key_type));
}

void wsrep_cert_key_history_append(THD *thd, const wsrep::key &key) try {
  mysql_mutex_lock(&thd->LOCK_thd_data);
  if (!thd->wsrep_cert_key_history) {
    thd->wsrep_cert_key_history= new Wsrep_cert_key_history;
  }
  thd->wsrep_cert_key_history->append(thd->thread_id, key);
  mysql_mutex_unlock(&thd->LOCK_thd_data);
} catch (...) {
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

bool wsrep_cert_key_history_foreach(THD *thd,
                        bool (*fn)(void *ctx, my_thread_id, const char *,
                                   Wsrep_service_key_type),
                        void *ctx) try {
  mysql_mutex_lock(&thd->LOCK_thd_data);
  if (thd->wsrep_cert_key_history) {
    thd->wsrep_cert_key_history->show(fn, ctx);
  }
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return true;
} catch (...) {
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return false;
}

bool wsrep_cert_key_history_foreach_history(bool (*fn)(void *ctx, my_thread_id, const char *,
                                          Wsrep_service_key_type),
                               void *ctx) try {
  Wsrep_cert_key_history::show_history(fn, ctx);
  return true;
} catch (...) {
  return false;
}

void wsrep_cert_key_history_init() {
  Wsrep_cert_key_history::initialized_count.fetch_add(1, std::memory_order_relaxed);
}

void wsrep_cert_key_history_deinit() {
  Wsrep_cert_key_history::initialized_count.fetch_sub(1, std::memory_order_relaxed);
  std::lock_guard<std::mutex> lock{Wsrep_cert_key_history::mutex};
  Wsrep_cert_key_history::keys_history.clear();
}
