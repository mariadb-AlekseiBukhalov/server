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

#ifndef WSREP_CERT_KEY_HISTORY_H
#define WSREP_CERT_KEY_HISTORY_H

/*
  Debug instrumentation for collecting wsrep keys to be show in information
  schema table.
*/

#include <my_global.h>
#include <sql_class.h>
#include <m_ctype.h>

#include "mysql/service_wsrep.h"
#include "wsrep.h"
#include "wsrep/key.hpp"
#include <deque>
#include <mutex>
#include <string>
#include <vector>
/**
  @class Wsrep_cert_key_history

  A class for collecting and managing WSREP keys during transaction processing.
  This class is used for debug instrumentation to track keys that are processed
  during WSREP operations. The keys are stored with their parts and type, and
  can be displayed in hex format for debugging purposes.

  The class maintains a collection of key entries, where each entry contains:
  - A vector of key parts (as strings)
  - The key type (Wsrep_service_key_type)

  This is primarily used for debugging and monitoring purposes, allowing
  inspection of keys processed during WSREP operations through the information
  schema.
*/
class Wsrep_cert_key_history {
 public:

  /**
    @struct Key_entry

    Internal structure to store a single key entry with its parts and type.

    @var key_parts Vector of strings containing the individual parts of the key
    @var key_type The type of the key (shared, exclusive, etc.)
  */
  struct Key_entry {
    my_thread_id thd_id;
    std::vector<std::string> key_parts;
    enum wsrep::key::type key_type;
  };

  static std::deque<Key_entry> keys_history;
  static std::mutex mutex;
  static std::atomic<size_t> initialized_count;

  /**
    Appends a new key to the key set.

    @param key The WSREP key to append
    @param key_type The type of the key (shared, exclusive, etc.)

    The key parts are converted to strings and stored along with the key type.
    This operation is not thread-safe and should be protected by appropriate
    synchronization when used in a multi-threaded context.
  */
  void append(my_thread_id thd_id, const wsrep::key &key);

  static void append_history(const Key_entry &key_entry);

  /**
    Displays all keys in the set by converting them to hex strings.

    @param fn Callback function that receives each key as a hex string and its
    type

    For each key in the set:
    1. Converts each key part to a hex string
    2. Calls the provided callback function with the hex string and key type

    This operation is not thread-safe and should be protected by appropriate
    synchronization when used in a multi-threaded context.
  */
  void show(bool (*fn)(void *ctx, my_thread_id, const char *, Wsrep_service_key_type),
            void *ctx) {
    for (const auto &key : keys_) {
      show_one_key(key, fn, ctx);
    }
  }

  /**
    Displays all keys in the history by converting them to hex strings.

    @param fn Callback function that receives each key as a hex string and its
    type

    This operation is thread-safe and can be used in a multi-threaded context.
  */
    static void show_history(bool (*fn)(void *ctx, my_thread_id, const char *, Wsrep_service_key_type),
                           void *ctx);


 private:
  static void show_one_key(const Key_entry &key_entry,
                           bool (*fn)(void *ctx, my_thread_id, const char *,
                                      Wsrep_service_key_type),
                           void *ctx);
  std::vector<Key_entry> keys_{};  ///< Collection of key entries
};

/**
  Thread-safe wrapper to append a key to a thread's key set.

  @param thd The thread handle
  @param key The WSREP key to append
  @param key_type The type of the key

  Creates a new key set if none exists for the thread and appends the key
  while holding the thread's WSREP mutex for thread safety.
*/
void wsrep_cert_key_history_append(THD *thd, const wsrep::key &key);

/**
  Thread-safe wrapper to show all keys in a thread's key set.

  @param thd The thread handle
  @param fn Callback function to process each key

  Shows all keys while holding the thread's WSREP mutex for thread safety.
  If no key set exists for the thread, nothing is done.

  @return true if the key set was shown, false if there was an error
*/
bool wsrep_cert_key_history_foreach(THD *thd,
                        bool (*fn)(void *ctx, my_thread_id, const char *,
                                   Wsrep_service_key_type),
                        void *ctx);

/**
  Thread-safe wrapper to show all keys in the history.

  @param fn Callback function to process each key

  Shows all keys while holding the thread's WSREP mutex for thread safety.
*/
bool wsrep_cert_key_history_foreach_history(bool (*fn)(void *ctx, my_thread_id, const char *,
                                          Wsrep_service_key_type),
                               void *ctx);

/**
  Initialize the key set instrumentation. After this call, the key set will be
  collected and displayed in the information schema table.
*/
void wsrep_cert_key_history_init();

/**
  Deinitialize the key set instrumentation. After this call, the key set will
  not be collected and displayed in the information schema table.
*/
void wsrep_cert_key_history_deinit();

/**
  Check if the key set instrumentation is initialized.
*/
static inline bool wsrep_cert_key_history_is_initialized() {
  return Wsrep_cert_key_history::initialized_count.load(std::memory_order_relaxed) > 0;
}

#endif /* WSREP_CERT_KEY_HISTORY_H */
