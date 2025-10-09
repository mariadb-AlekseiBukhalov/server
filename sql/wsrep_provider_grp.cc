/* Copyright 2022 Codership Oy <info@codership.com>

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

#include "wsrep_provider_grp.h"

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <regex>
#include <thread>

#include "wsrep/allowlist_service.hpp"
#include "wsrep/exception.hpp"
#include "wsrep/high_priority_service.hpp"

#include <my_global.h>
#include <mysql/plugin_galera_replication.h>

#include "debug_sync.h"
#include "sql_class.h"
#include "log.h"

#include "wsrep_galera_replication.h"
#include "wsrep_binlog.h"
#include "wsrep_high_priority_service.h"
#include "wsrep_mysqld.h"
#include "wsrep_server_state.h"
#include "wsrep_schema.h"
#include "wsrep_thd.h"
#include "wsrep_xid.h"

#ifdef HAVE_PSI_INTERFACE
/* Defined in mysqld.cc */
extern PSI_socket_key key_socket_grp;
/* Defined in wsrep_mysqld.cc */
extern PSI_thread_key key_grp_service_thread;
#endif /* HAVE_PSI_INTERFACE */

namespace
{
/* */
struct Grp_provider_position_guard
{
  Grp_provider_position_guard(THD* thd, long long provider_position)
    : m_thd{thd}
  {
    m_thd->wsrep_grp_log_position= provider_position;
  }
  ~Grp_provider_position_guard() {
    m_thd->wsrep_grp_log_position= 0;
  }
  THD* m_thd;
};

std::string strip(std::string str)
{
  str.erase(remove_if(str.begin(), str.end(), isspace), str.end());
  return str;
}

/* Helper for string splitting */
std::vector<std::string> split(const std::string &str, char delim)
{
  std::vector<std::string> ret;
  std::stringstream ss{str};
  std::string token;
  while (std::getline(ss, token, delim))
  {
    ret.push_back(strip(token));
  }
  return ret;
}

#if defined(ENABLED_DEBUG_SYNC)
/*
 * Rudimentary debug sync facility to provide Galera compatible
 * debug syncing via wsrep_provider_options.
 */

/* Mutex for sync wait instrumentation protection */
std::mutex sync_mutex;
/* Active sync points */
std::multimap<std::string, std::condition_variable *> sync_waiters;
/* Known sync points for sanity checks. Assert every time unknown
   sync point is encountered to speed up detecting unimplemented cases. */
std::set<std::string> known_sync_points= {
  "abort_trx_end",
  "after_certify_apply_monitor_enter",
  "after_replicate_sync",
  "after_shift_to_joining",
  "apply_monitor_master_enter_sync",
  "before_certify_apply_monitor_enter",
  "before_local_commit_monitor_enter",
  "before_replicate_sync",
  "before_send_state_request",
  "apply_monitor_slave_enter_sync",
  "commit_monitor_master_enter_sync",
  "commit_monitor_slave_enter_sync",
  "local_monitor_master_enter_sync",
  "local_monitor_slave_enter_sync",
  "process_primary_configuration",
  "recv_IST_after_apply_trx"
};
/* Active sync points */
std::set<std::string> sync_points;

void debug_sync_set_dbug(Wsrep_provider_grp*, const std::string &val_str)
{
  auto val= split(val_str, ',');
  if (val.empty())
  {
    sync_points.clear();
  }
  else
  {
    DBUG_ASSERT(val.size() == 2);
    auto it = known_sync_points.find(val[1]);
    if (it == known_sync_points.end()) {
      wsrep::log_error() << "Unknown sync point: " << val[1];
      DBUG_ASSERT(0);
    }
    sync_points.insert(val[1]);
  }
}

void debug_sync_signal(Wsrep_provider_grp*, const std::string &sync)
{
  std::unique_lock<std::mutex> lock{sync_mutex};
  auto range= sync_waiters.equal_range(sync);
  for (auto i= range.first; i != range.second; ++i)
  {
    i->second->notify_one();
  }
}

void debug_sync_wait(const char *sync)
{
  DBUG_ASSERT(known_sync_points.find(sync) != known_sync_points.end());
  std::unique_lock<std::mutex> lock{sync_mutex};
  if (sync_points.find(sync) == sync_points.end())
  {
    /* Sync point not active */
    return;
  }

  WSREP_DEBUG("debug_sync_wait enter: %s", sync);
  std::condition_variable cond;
  auto i= sync_waiters.insert(std::make_pair(sync, &cond));
  cond.wait(lock);
  sync_waiters.erase(i);
  WSREP_DEBUG("debug_sync_wait leave: %s", sync);
}

/*
  Convenience methods to wait for sync points where it is not
  clear from context whether the transaction is local or remote.
 */
bool is_local(THD *thd);

void local_sync_wait(THD *thd, const char *sync)
{
  if (is_local(thd))
  {
    debug_sync_wait(sync);
  }
}

void remote_sync_wait(THD *thd, const char *sync)
{
  if (!is_local(thd))
  {
    debug_sync_wait(sync);
  }
}

std::string debug_sync_waiters()
{
  std::string ret;
  std::unique_lock<std::mutex> lock{sync_mutex};
  for (auto i= sync_waiters.begin(); i != sync_waiters.end();)
  {
    ret+= i->first;
    ++i;
    if (i != sync_waiters.end())
      ret+= " ";
  }
  return ret;
}

#else
#define debug_sync_wait(S)
#define local_sync_wait(T, S)
#define remote_sync_wait(T, S)
#endif /* ENABLED_DEBUG_SYNC */

/*
  Replication handlers.
 */

enum wsrep::view::status get_wsrep_view_status(const grp_view_t *grp_view)
{
  switch (grp_view->type)
  {
  case GRP_VIEW_PRIMARY:
    return wsrep::view::status::primary;
  case GRP_VIEW_NON_PRIMARY:
    return wsrep::view::status::non_primary;
  case GRP_VIEW_DISCONNECTED:
    return wsrep::view::status::disconnected;
  }
  return wsrep::view::status::disconnected;
}

wsrep::id make_wsrep_id(const grp_id_t &grp_id)
{
  return wsrep::id{grp_id.data, grp_id.size};
}

wsrep::seqno make_wsrep_seqno(grp_seqno_t seqno)
{
  if (seqno > 0) {
    return wsrep::seqno{seqno};
  }
  return wsrep::seqno::undefined();
}

constexpr int provider_capabilities=
    wsrep::provider::capability::multi_master |
    wsrep::provider::capability::certification |
    wsrep::provider::capability::parallel_applying |
    wsrep::provider::capability::transaction_replay |
    wsrep::provider::capability::isolation |
    wsrep::provider::capability::pause |
    wsrep::provider::capability::causal_reads |
    wsrep::provider::capability::streaming;

wsrep::view make_wsrep_view(const grp_view_t *grp_view)
{
  wsrep::seqno view_seqno= make_wsrep_seqno(grp_view->seqno);
  wsrep::gtid state_id{make_wsrep_id(grp_view->view_id), view_seqno};
  std::vector<wsrep::view::member> members;

  for (size_t i= 0; i < grp_view->members.size; ++i)
  {
    auto id{make_wsrep_id(grp_view->members.members[i].id)};
    members.push_back(
        wsrep::view::member{id, grp_view->members.members[i].name,
                            grp_view->members.members[i].incoming_address});
  }

  wsrep::view view{state_id,
                   view_seqno,
                   get_wsrep_view_status(grp_view),
                   provider_capabilities,
                   grp_view->own_index,
                   (int) wsrep_protocol_version,
                   members};
  return view;
}

wsrep::ws_meta make_wsrep_ws_meta(const grp_write_set_t &write_set,
                                  wsrep::stid stid, int flags)
{
    return {wsrep::gtid{make_wsrep_id(write_set.cert_meta.cluster_id),
                      wsrep::seqno{write_set.cert_meta.seqno}},
          std::move(stid), wsrep::seqno{write_set.cert_meta.depends_on}, flags};
}

enum wsrep::provider::status
make_wsrep_cert_status(const grp_cert_result_t &cert_result)
{
  switch (cert_result)
  {
  case GRP_CERT_SUCCESS:
    return wsrep::provider::success;
  case GRP_CERT_CONFLICT:
    return wsrep::provider::error_certification_failed;
  case GRP_CERT_REJECT:
    return wsrep::provider::error_not_allowed;
  case GRP_CERT_SKIP:
    return wsrep::provider::error_unknown;
  case GRP_CERT_CANCEL:
    return wsrep::provider::error_connection_failed;
  case GRP_CERT_NONE:
    return wsrep::provider::error_provider_failed;
  }
  MY_ASSERT_UNREACHABLE();
}

grp_const_buf_t make_grp_const_buf(const wsrep::const_buffer &buffer)
{
  return {buffer.ptr(), buffer.size()};
}

grp_ws_kind_t make_grp_kind(int flags)
{
  const auto start= wsrep::starts_transaction(flags);
  const auto commit= wsrep::commits_transaction(flags);
  const auto rollback= wsrep::rolls_back_transaction(flags);

  if (wsrep::is_toi(flags))
  {
    DBUG_ASSERT(!rollback);
    if (start && commit)
    {
      return GRP_KIND_TOI;
    }
    else if (start)
    {
      return GRP_KIND_NBO_BEGIN;
    }
    else
    {
      return GRP_KIND_NBO_END;
    }
  }
  else if (start && commit)
  {
    return GRP_KIND_TRX_ATOMIC;
  }
  else if (start && !commit)
  {
    DBUG_ASSERT(!rollback);
    return GRP_KIND_TRX_START;
  }
  else if (!start && commit)
  {
    DBUG_ASSERT(!rollback);
    return GRP_KIND_TRX_COMMIT;
  }
  else if (!start && rollback)
  {
    return GRP_KIND_TRX_ROLLBACK;
  }
  else if (!start && !commit && !rollback)
  {
    return GRP_KIND_TRX_FRAGMENT;
  }
  else if (wsrep::prepares_transaction(flags))
  {
    return GRP_KIND_TRX_PREPARE;
  }
  else
  {
    MY_ASSERT_UNREACHABLE();
  }
}

int handle_connected(grp_handler_context_t *context, const grp_view_t *view)
{
  auto provider= (Wsrep_provider_grp *) context;
  return provider->handle_connected(make_wsrep_view(view));
}

int handle_view(grp_handler_context_t *context, const grp_view_t *view)
{
  auto provider= (Wsrep_provider_grp *) context;
  return provider->handle_view(make_wsrep_view(view), view->provider_position);
}

int handle_write_set(grp_handler_context_t *context,
                     grp_write_set_t *write_set, bool is_local)
{
  auto provider= (Wsrep_provider_grp *) context;
  return provider->handle_write_set(write_set, is_local);
}

int handle_error_vote(grp_handler_context_t *context,
                      grp_apply_error_t *error_vote,
                      grp_vote_result_t result,
                      bool is_local)
{
  auto provider= (Wsrep_provider_grp *) context;
  return provider->handle_error_vote(error_vote, result, is_local);
}

int handle_sync(grp_handler_context_t *context)
{
  auto provider= (Wsrep_provider_grp *) context;
  return provider->handle_sync();
}

int handle_sst_request(grp_handler_context_t *context,
                       const char *method,
                       const char *receive_address)
{
  auto provider= (Wsrep_provider_grp *) context;
  return provider->handle_sst_request(method, receive_address);
}

grp_seqno_t get_last_committed(const grp_handler_context_t *context)
{
  auto provider= (const Wsrep_provider_grp *) context;
  return provider->last_committed().get();
}

bool allowlist_check(grp_handler_context_t *context,
                     grp_allowlist_key_t key,
                     grp_const_buf_t value)
{
  auto provider= (Wsrep_provider_grp *) context;
  if (auto allowlist_service= provider->services().allowlist_service)
  {
    return allowlist_service->allowlist_cb(
        static_cast<wsrep::allowlist_service::allowlist_key>(key),
        wsrep::const_buffer{value.data, value.size});
  }
  return true;
}

void log_message(grp_log_level_t level, const char* msg)
{
  switch (level)
  {
  case GRP_LOG_ERROR:
    sql_print_error("GRP: %s", msg);
    break;
  case GRP_LOG_WARN:
    sql_print_warning("GRP: %s", msg);
    break;
  case GRP_LOG_INFO:
    sql_print_information("GRP: %s", msg);
    break;
  case GRP_LOG_DEBUG:
    sql_print_information("GRP[debug]: %s", msg);
    break;
  }
}

PSI_socket_key get_psi_socket_key()
{
#ifdef HAVE_PSI_INTERFACE
  return key_socket_grp;
#else
  return 0;
#endif
}

extern "C" void *run_grp_service_thread(void *arg)
{
  my_thread_set_name("grp_service");
  auto grp= (st_mysql_galera_replication *) arg;
  const int err= grp->run();
  if (err)
  {
    WSREP_ERROR("Galera replication service thread returned %d", err);
  }
  return nullptr;
}

struct Key_set_node
{
  std::map<std::string, std::unique_ptr<Key_set_node>> paths;
  int16_t type{0};
};

class Wsrep_write_set : public grp_write_set_t
{
public:
  Wsrep_write_set() : Wsrep_write_set(0, 0) {}
  explicit Wsrep_write_set(wsrep::seqno::native_type read_view,
                           wsrep::transaction_id::type trx_id)
      : Wsrep_write_set({nullptr, 0}, read_view, trx_id)
  {
    this->kind= GRP_KIND_NONE;
  }
  explicit Wsrep_write_set(grp_const_buf_t buffer,
                           wsrep::seqno::native_type read_view,
                           wsrep::transaction_id::type trx_id)
      : m_thd(current_thd), m_mutex{}, m_cond_sent{},
        m_cond_cert{}, m_state{State::None}, m_keys{},
        m_keys_count{1}, m_keys_bytes{0}, m_data{}
  {
    this->client_id= m_thd->thread_id;
    this->transaction_id= trx_id;
    this->read_view= read_view;
    this->last_committed= wsrep::seqno::undefined().get();
    this->kind= GRP_KIND_TOI;
    init_mem_root();
    memset(&this->keys, 0, sizeof(this->keys));
    this->data= buffer;
    this->cert_result= GRP_CERT_NONE;
  }

  Wsrep_write_set(Wsrep_write_set &&)= delete;
  Wsrep_write_set &operator=(Wsrep_write_set &&)= delete;

  ~Wsrep_write_set() {
    DBUG_ASSERT(m_conflict.is_undefined() && "provider::release() not called");
    free_mem_root();
  }

  void append_key(const wsrep::key &key)
  {
    auto *current_node= &m_keys;
    for (size_t i= 0; i < key.size(); ++i)
    {
      ++m_keys_count;
      m_keys_bytes += key.key_parts()[i].size();
      // Determine proper key type for the node:
      // - non-leaf nodes have shared key type;
      // - for existing node, the strongest key type is stored;
      // - child nodes of the exclusive node may be dropped.
      // - TOI leaf keys are always exclusive. This is not optimal,
      //   but it is the way how Galera library behaves, so keeping the
      //   backwards compatibility for now.
      const auto key_type= static_cast<int16_t>(
          (i == key.size() - 1)
              ?
              (this->kind == GRP_KIND_TOI ? wsrep::key::exclusive : key.type())
              : wsrep::key::reference);

      auto &node= current_node->paths[std::string(
          (const char *) key.key_parts()[i].ptr(), key.key_parts()[i].size())];
      if (!node)
      {
        node.reset(new Key_set_node());
        node->type= key_type;
      }
      else
      {
        node->type= std::max(node->type, key_type);
      }

      current_node= node.get();
    }
  }

  void append_data(const wsrep::const_buffer &buffer)
  {
    m_data.insert(m_data.end(), buffer.data(), buffer.data() + buffer.size());
  }

  bool finalize(wsrep::seqno::native_type last_committed, int flags)
  {
    DBUG_ASSERT((this->last_committed == wsrep::seqno::undefined().get() ||
                 !(flags & wsrep::provider::flag::start_transaction)) &&
                    "finalize() called twice");

    this->last_committed= last_committed;
    this->kind= make_grp_kind(flags);
    if (this->kind == GRP_KIND_TRX_FRAGMENT ||
        this->kind == GRP_KIND_TRX_COMMIT) {
      this->read_view = last_committed;
    }
    this->pa_policy= (flags & wsrep::provider::flag::pa_unsafe) ? GRP_PA_UNSAFE
                                                                : GRP_PA_SAFE;
    this->keys= build_keys();
    /* Don't override data if it was assigned in constructor. */
    if (this->data.data == nullptr)
    {
      // It's not specified if empty vector returns nullptr or not.
      this->data= {m_data.empty() ? nullptr : m_data.data(), m_data.size()};
    }
    else
    {
      DBUG_ASSERT(m_data.empty());
    }

    std::lock_guard<std::mutex> lk(m_mutex);
    return !m_conflict.is_undefined();
  }

  /**
     Notify the replication service thread that the write set
     has been sent and the local thread has started waiting for
     the result. This method will block until the certification
     result is available or the wait is interrupted by BF
     aborter.

     @return True if the wait was interrupted, otherwise false.
   */
  bool notify_sent_and_wait_certified()
  {
    DBUG_ASSERT(this->last_committed != wsrep::seqno::undefined().get() &&
                "finalize() not called");

    debug_sync_wait("after_replicate_sync");
    debug_sync_wait("local_monitor_master_enter_sync");

    std::unique_lock<std::mutex> lk(m_mutex);
    DBUG_ASSERT(m_state != State::Sent &&
                "notify_sent_and_wait_certified() called twice");
    if (m_state == State::None)
    {
      m_state= State::Sent;
      m_cond_sent.notify_one();
    }

    while (m_state != State::Certified && m_conflict.is_undefined())
    {
      m_cond_cert.wait(lk);
    }

    DBUG_ASSERT(!m_conflict.is_undefined() || cert_result != GRP_CERT_NONE);
    return !m_conflict.is_undefined() && m_state != State::Certified;
  }

  void wait_sent()
  {
    std::unique_lock<std::mutex> lk(m_mutex);
    while (m_state != State::Sent)
    {
      m_cond_sent.wait(lk);
    }
  }

  /**
     Wait for certification result after the write set has been sent.
     This method will block until the result is available and cannot
     be interrupted.
   */
  void wait_certified()
  {
    std::unique_lock<std::mutex> lk(m_mutex);
    DBUG_ASSERT(m_state != State::None);
    wait_certified(lk);
  }

  /**
     Wait for certification result if the write set has been sent.
     This method will block until the result is available and cannot
     be interrupted.
   */
  void wait_certified_if_sent()
  {
    std::unique_lock<std::mutex> lk(m_mutex);
    if (m_state == State::None)
    {
      return;
    }
    wait_certified(lk);
  }

  void notify_certified()
  {
    std::lock_guard<std::mutex> lk(m_mutex);
    m_state= State::Certified;
    m_cond_cert.notify_one();
  }

  /**
     Interrupt the certification result wait and set conflicting seqno
     for this write set.
  */
  void interrupt(wsrep::seqno conflict_seqno)
  {
    std::lock_guard<std::mutex> lk{m_mutex};
    update_conflict(lk, conflict_seqno);
    if (m_state == State::Sent)
    {
      m_cond_cert.notify_one();
    }
  }

  /**
     Notify about a certification conflict and set conflicting seqno
     for this write set.
  */
  void notify_conflict(wsrep::seqno conflict_seqno)
  {
    std::lock_guard<std::mutex> lk{m_mutex};
    update_conflict(lk, conflict_seqno);
    m_state = State::Certified;
    m_cond_cert.notify_one();
  }

  /**
     Query conflicting seqno.
  */
  wsrep::seqno conflict_seqno() &
  {
    std::lock_guard<std::mutex> lk{m_mutex};
    return m_conflict;
  }

  /**
     Take out conflicting seqno.
  */
  wsrep::seqno conflict_seqno() &&
  {
    std::lock_guard<std::mutex> lk{m_mutex};
    auto tmp = m_conflict;
    m_conflict = wsrep::seqno::undefined();
    return tmp;
  }

  THD *thd() const { return m_thd; }

  void reset_for_next_fragment()
  {
    m_keys_count= 1;
    m_keys_bytes= 0;
    m_state= State::None;
    cert_result= GRP_CERT_NONE;
    free_mem_root();
    m_keys.paths.clear();
    m_keys.type= 0;
    this->pa_policy= GRP_PA_SAFE;
    this->keys= grp_key_set_node_t{};
    this->data= grp_const_buf_t{};
    m_data.clear();
  }

  void update_status(Wsrep_provider_grp::Status& status)
  {
    ++status.replicated;
    status.repl_keys+= m_keys_count;
    status.repl_keys_bytes+= m_keys_bytes;
    status.repl_data_bytes+= m_data.size();
  }
private:
  void init_mem_root()
  {
    init_alloc_root(PSI_NOT_INSTRUMENTED, &m_keys_mem_root, 1024,
                    0, MYF(0));
  }
  void free_mem_root()
  {
    free_root(&m_keys_mem_root, MYF(0));
  }
  grp_key_set_node_t build_keys() const
  {
    // Use breadth-first traversal to allocate children as a contiguous array.

    struct Key_node_traverse
    {
      const std::string *path;
      const Key_set_node *key_node_from;
      grp_key_set_node_t *key_node_to;
    };

    grp_key_set_node_t key_set;

    std::queue<Key_node_traverse> key_nodes;
    key_nodes.push({nullptr, &m_keys, &key_set});

    // TODO(denis): Truncate child nodes under the exclusive node type.

    while (!key_nodes.empty())
    {
      const auto *key_node_from= key_nodes.front().key_node_from;
      auto *key_node_to= key_nodes.front().key_node_to;
      const auto *path= key_nodes.front().path;
      key_nodes.pop();

      key_node_to->path= {path ? path->data() : nullptr,
                          path ? path->size() : 0};
      key_node_to->type= key_node_from->type;
      key_node_to->nodes= (grp_key_set_node_t *) alloc_root(
          &m_keys_mem_root,
          sizeof(grp_key_set_node_t) * key_node_from->paths.size());
      key_node_to->nodes_size= key_node_from->paths.size();

      size_t i= 0;
      for (const auto &kv : key_node_from->paths)
      {
        key_nodes.push({&kv.first, kv.second.get(), &key_node_to->nodes[i]});
        ++i;
      }
    }

    return key_set;
  }

  void update_conflict(std::lock_guard<std::mutex> &,
                       wsrep::seqno conflict_seqno)
  {
    DBUG_ASSERT(!conflict_seqno.is_undefined());
    m_conflict = std::max(m_conflict, conflict_seqno);
  }

  void wait_certified(std::unique_lock<std::mutex>& lk)
  {
    while (m_state != State::Certified)
    {
      m_cond_cert.wait(lk);
    }
  }

private:
  enum State
  {
    None,
    Sent,
    Certified,
  };

  THD *m_thd;

  std::mutex m_mutex;
  std::condition_variable m_cond_sent;
  std::condition_variable m_cond_cert;
  State m_state;
  // Conflicting write set seqno, if any.
  wsrep::seqno m_conflict;

  // Memory root for key nodes
  mutable MEM_ROOT m_keys_mem_root;
  // Root node of the key set tree.
  Key_set_node m_keys;
  /* Statistics counter for appended key parts. This is initialized to
   * one to count the root key in. This follows Galera library key part
   * counting convention. */
  size_t m_keys_count;
  /* Statistics counter for appended key part bytes. */
  size_t m_keys_bytes;
  // Holds the collected write set data for certification.
  std::vector<char> m_data;
};

#if defined(ENABLED_DEBUG_SYNC)
bool is_local(THD *thd)
{
  auto ws= static_cast<Wsrep_write_set*>(thd->wsrep_trx().ws_handle().opaque());
  return thd->wsrep_cs().mode() == wsrep::client_state::m_local ||
    (ws && ws->thd() && ws->thd() != thd);
}
#endif /* ENABLED_DEBUG_SYNC */

} // namespace

int make_wsrep_flags(grp_ws_kind_t kind)
{
  switch (kind)
  {
  case GRP_KIND_TOI:
    return wsrep::provider::flag::isolation |
           wsrep::provider::flag::start_transaction |
           wsrep::provider::flag::commit;
  case GRP_KIND_NBO_BEGIN:
    return wsrep::provider::flag::isolation |
           wsrep::provider::flag::start_transaction;
  case GRP_KIND_NBO_END:
    return wsrep::provider::flag::isolation | wsrep::provider::flag::commit;
  case GRP_KIND_TRX_ATOMIC:
    return wsrep::provider::flag::start_transaction |
           wsrep::provider::flag::commit;
  case GRP_KIND_TRX_START:
    return wsrep::provider::flag::start_transaction;
  case GRP_KIND_TRX_FRAGMENT:
    // TODO(denis): Do we set any flags here?
    return 0;
  case GRP_KIND_TRX_PREPARE:
    return wsrep::provider::flag::prepare;
  case GRP_KIND_TRX_COMMIT:
    return wsrep::provider::flag::commit;
  case GRP_KIND_TRX_ROLLBACK:
    return wsrep::provider::flag::rollback;
  default:
    MY_ASSERT_UNREACHABLE();
  }
}

static Wsrep_provider_grp* provider_ptr= nullptr;

Wsrep_provider_grp *get_provider_grp()
{
  return provider_ptr;
}

#include <regex>

int validate_grp_cluster_address(const std::string &cluster_url)
{
  static const std::regex cluster_address_regex(
      R"(^((gcomm://)?(\[?[a-zA-Z0-9:.-]+\]?(:[0-9]+)?)?(,\[?[a-zA-Z0-9:.-]+\]?(:[0-9]+)?)*)$)");

  if (std::regex_match(cluster_url, cluster_address_regex))
  {
    return 0; // Valid cluster address
  }
  return 1; // Invalid cluster address
}

std::string make_grp_cluster_address(const std::string &cluster_url)
{
  static const char *gcomm_prefix= "gcomm://";

  std::string cluster_address;
  const auto prefix_url= cluster_url.find(gcomm_prefix);
  if (prefix_url == 0)
  {
    cluster_address= cluster_url.substr(strlen(gcomm_prefix));
  }
  else
  {
    cluster_address= cluster_url;
  }
  return cluster_address;
}

std::unique_ptr<wsrep::provider>
Wsrep_provider_grp::make_provider(wsrep::server_state &server_state,
                                  const wsrep::provider::services& services)
{
  if (!wsrep_is_galera_plugin_loaded())
  {
    WSREP_ERROR(
        "Galera replication plugin is not loaded. Leaving provider empty.");
    return std::unique_ptr<wsrep::provider>();
  }

  auto *grp= wsrep_get_galera_plugin_descriptor();
  if (!grp)
  {
    WSREP_ERROR("Could not get Galera replication plugin descriptor");
    return std::unique_ptr<wsrep::provider>();
  }

  auto provider= std::unique_ptr<wsrep::provider>(
      new Wsrep_provider_grp(server_state, services, grp));

  // Build a static singleton handler to serve calls from the GRP plugin into
  // the provider.
  static grp_handler_t grp_handler{::handle_connected,
                                   ::handle_view,
                                   ::handle_write_set,
                                   ::handle_error_vote,
                                   ::handle_sync,
                                   ::handle_sst_request,
                                   ::get_last_committed,
                                   ::allowlist_check,
                                   ::log_message,
                                   ::get_psi_socket_key,
                                   (grp_handler_context_t *) provider.get()};
  if (grp->init(server_state.address().c_str(), &grp_handler))
  {
    WSREP_ERROR("Could not get Galera replication plugin");
    return std::unique_ptr<wsrep::provider>();
  }

  static pthread_t grp_service_thread;

  if (mysql_thread_create(key_grp_service_thread, &grp_service_thread, nullptr,
                          run_grp_service_thread, grp))
  {
    WSREP_ERROR("Failed to start Galera replication service thread");
    grp->deinit();
    return std::unique_ptr<wsrep::provider>();
  }

  return provider;
}

Wsrep_provider_grp::Wsrep_provider_grp(wsrep::server_state &server_state,
                                       const wsrep::provider::services& services,
                                       st_mysql_galera_replication *grp)
    : wsrep::provider{server_state}, m_events_mutex{}, m_events_cond{},
      m_events{}, m_isolated_in_progress{false}, m_closed{false},
      m_last_received{}, m_last_committed{0},
      m_last_committed_low{} /* Constructed as undefined seqno */,
      m_last_committed_waiters{}, m_desync_state{Desync_state::IDLE},
      m_pause_seqno{}, m_pause_state{Pause_state::IDLE}, m_services{services},
      m_status{}, m_grp{grp}
{
#ifdef HAVE_PSI_INTERFACE
  static PSI_mutex_key key_grp_events_mutex;
  static PSI_cond_key key_grp_events_cond;
  static PSI_mutex_key key_grp_last_committed_mutex;
  static PSI_mutex_key key_grp_desync_mutex;
  static PSI_cond_key key_grp_desync_cond;
  static PSI_mutex_key key_grp_pause_mutex;
  static PSI_cond_key key_grp_pause_cond;
  static PSI_mutex_key key_grp_status_mutex;
#endif /* HAVE_PSI_INTERFACE */
  mysql_mutex_init(key_grp_events_mutex, &m_events_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_grp_events_cond, &m_events_cond, nullptr);
  mysql_mutex_init(key_grp_last_committed_mutex, &m_last_committed_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_grp_desync_mutex, &m_desync_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_grp_desync_cond, &m_desync_cond, nullptr);
  mysql_mutex_init(key_grp_pause_mutex, &m_pause_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_grp_pause_cond, &m_pause_cond, nullptr);
  mysql_mutex_init(key_grp_status_mutex, &m_status_mutex, MY_MUTEX_INIT_FAST);
  provider_ptr= this;
}

Wsrep_provider_grp::~Wsrep_provider_grp()
{
  provider_ptr= nullptr;
  m_grp->deinit();
  mysql_mutex_destroy(&m_events_mutex);
  mysql_cond_destroy(&m_events_cond);
  mysql_mutex_destroy(&m_last_committed_mutex);
  mysql_mutex_destroy(&m_desync_mutex);
  mysql_cond_destroy(&m_desync_cond);
  mysql_mutex_destroy(&m_pause_mutex);
  mysql_cond_destroy(&m_pause_cond);
  mysql_mutex_destroy(&m_status_mutex);
}

wsrep::provider::services& Wsrep_provider_grp::services()
{
  return m_services;
}


int Wsrep_provider_grp::handle_connected(wsrep::view &&view)
try
{
  queue_event(new drain_event{
      [view, this](Wsrep_provider_grp &self) {
        if (!m_sr_write_sets_recovered) {
          if (wsrep_schema->recover_sr_transactions(current_thd)) {
            /* Caught by drain_event::apply() */
            throw wsrep::runtime_error("Failed to recover SR transactions");
          }
          m_sr_write_sets_recovered= true;
        }
        self.server_state_.on_connect(view);
      },
      m_last_received});
  return 0;
}
catch (const wsrep::runtime_error &e)
{
  wsrep::log_warning() << "Failed to handle connected: " << e.what();
  return 1;
}

wsrep::seqno Wsrep_provider_grp::update_last_received(wsrep::seqno seqno)
{
  if (seqno.is_undefined())
  {
    return m_last_received;
  }

  DBUG_ASSERT(m_last_received.is_undefined() || m_last_received == seqno.prev());
  auto ret= m_last_received;
  m_last_received= seqno;
  return ret;
}

int Wsrep_provider_grp::handle_view(wsrep::view &&view, long long provider_position)
try
{
  /* Primary views have always seqno assigned. Non-primary view may or may not
     have seqno assigned, depending on how it was generated. If the non-Primary
     was delivered because the connection to the cluster was lost, it will not
     have valid seqno. If the non-Primary was generated because of cluster configuration
     change, the seqno is valid. */
  const auto wait_seqno= !view.view_seqno().is_undefined()
    ? update_last_received(view.view_seqno())
    : m_last_received;

  /* This is the first received view event, initialize m_last_committed_seqno
     to previous seqno. This happens in single threaded execution and the
     apply queue provides memory barrier. */
  if (m_last_committed_low.is_undefined() && !view.view_seqno().is_undefined())
  {
    m_last_committed_low= view.view_seqno().prev();
  }

  const auto view_status= view.status();
  queue_event(new view_event(std::move(view), wait_seqno, provider_position,
                             view_status == wsrep::view::disconnected));
  return 0;
}
catch (const wsrep::runtime_error &e)
{
  wsrep::log_warning() << "Failed to handle view: " << e.what();
  return 1;
}

void Wsrep_provider_grp::queue_write_set_for_applying(
    const grp_write_set_t *write_set)
{
  DBUG_ASSERT(write_set->cert_result == GRP_CERT_SUCCESS);
  update_last_received(wsrep::seqno{write_set->cert_meta.seqno});
  /* Initialize m_last_committed_low if the first applied event with
     valid seqno is remote write set. This happens in single threaded
     execution and the apply queue provides memory barrier.*/
  if (m_last_committed_low.is_undefined())
  {
    m_last_committed_low= wsrep::seqno{write_set->cert_meta.seqno}.prev();
  }
  ++m_status.received;
  /* Note that we see here only write set data size, so this is not the
   * same as Galera wsrep_received_bytes. For better statistics, the
   * total size should be populated in the write set by replication plugin. */
  m_status.received_bytes += write_set->data.size;
  auto remote= new remote_certified_event(*write_set);
  wsrep_grp_register_for_group_commit(remote->wfc());
  queue_event(remote);
}

int Wsrep_provider_grp::handle_write_set(grp_write_set_t *write_set,
                                         bool is_local)
try
{
  DBUG_ASSERT(write_set);

  /* Update the certification statistics. */
  if (write_set->cert_result == GRP_CERT_SUCCESS)
  {
    const auto& cert_meta = write_set->cert_meta;
    DBUG_ASSERT(cert_meta.seqno > 0 && cert_meta.depends_on >= 0);
    ++m_status.cert_total;

    DBUG_ASSERT(cert_meta.seqno > cert_meta.depends_on);
    m_status.cert_deps_distance+= cert_meta.seqno - cert_meta.depends_on;

    DBUG_ASSERT(cert_meta.seqno > write_set->last_committed);
    m_status.cert_interval+= cert_meta.seqno -
                             (write_set->last_committed + 1);
  }

  if (is_local)
  {
    auto *ws= static_cast<Wsrep_write_set *>(write_set);

    /* Rollback write sets are always handled by applier */
    if (ws->kind == GRP_KIND_TRX_ROLLBACK)
    {
      if (ws->cert_result == GRP_CERT_SUCCESS)
      {
        queue_write_set_for_applying(write_set);
      }
      delete ws;
    }
    else if (ws->cert_result == GRP_CERT_SUCCESS)
    {
      update_last_received(wsrep::seqno{write_set->cert_meta.seqno});
      /* We should always handle either view event or remote write set
         before local write sets. Sanity check for that by checking that
         m_last_committed_low has been initialized. */
      DBUG_ASSERT(!m_last_committed_low.is_undefined());
      // Barrier to ensure that the local thread has blocked waiting in order
      // to safely access its wait_for_commit_ptr member.
      ws->wait_sent();
      ws->thd()->wait_for_commit_ptr= &ws->thd()->wsrep_wfc;
      wsrep_grp_register_for_group_commit(&ws->thd()->wsrep_wfc);
      ws->notify_certified();
    }
    else if (ws->cert_result == GRP_CERT_CONFLICT)
    {
      /* Set the conflicting seqno so that it may be waited for before
         retrying the transaction. */
      ws->notify_conflict(m_last_received);
    }
    else
    {
      ws->notify_certified();
    }
  }
  else if (write_set->cert_result == GRP_CERT_SUCCESS)
  {
    queue_write_set_for_applying(write_set);
  }
  else
  {
    DBUG_ASSERT(write_set->cert_result != GRP_CERT_NONE &&
                "Remote write set not certified");
  }

  /* The write set object may have already been freed after this point. */

  return 0;
}
catch (const wsrep::runtime_error &e)
{
  wsrep::log_warning() << "Failed to handle write set: " << e.what();
  return 1;
}

class Wsrep_apply_error : public grp_apply_error_t
{
public:
  Wsrep_apply_error(const char* data, size_t size, long long seqno, int64_t position) :
    grp_apply_error_t{ {data, size}, {{0,}, 0}, seqno, position },
    m_mutex{}, m_cond_result{}, m_result{GRP_VOTE_NONE} {}

  void wait_result()
  {
    std::unique_lock<std::mutex> lk(m_mutex);
    while (m_result == GRP_VOTE_NONE)
    {
      m_cond_result.wait(lk);
    }
  }

  void set_result(grp_vote_result_t result)
  {
    std::lock_guard<std::mutex> lk(m_mutex);
    m_result= result;
    m_cond_result.notify_one();
  }

  grp_vote_result_t result() const
  {
    std::lock_guard<std::mutex> lk(m_mutex);
    return m_result;
  }

  private:
  mutable std::mutex m_mutex;
  std::condition_variable m_cond_result;
  grp_vote_result_t m_result;
};

std::ostream &operator<<(std::ostream &os, grp_vote_result_t res)
{
  switch (res)
  {
  case GRP_VOTE_PASSED:
    os << "PASSED";
    return os;
  case GRP_VOTE_REJECTED:
    os << "REJECTED";
    return os;
  case GRP_VOTE_CANCELED:
    os << "CANCELED";
    return os;
  case GRP_VOTE_NONE:
    os << "NONE";
    return os;
  }
  os << "<undefined>";
  return os;
}

std::ostream &operator<<(std::ostream &os, const Wsrep_apply_error &ae)
{
  os << "{'";
  if (ae.data.size > 0)
  {
    os << static_cast<const char *>(ae.data.data);
  }
  os << "'[" << ae.data.size << "], seqno: " << ae.seqno
     << ", pos: " << ae.position << ", result: " << ae.result() << " }";
  return os;
}

int Wsrep_provider_grp::handle_error_vote(grp_apply_error_t* apply_error,
                                          grp_vote_result_t result,
                                          bool const is_local)
try
{
  DBUG_ASSERT(apply_error);

  if (is_local)
  {
    auto *ae= static_cast<Wsrep_apply_error *>(apply_error);
    ae->set_result(result);
  }
  return 0;
}
catch (const wsrep::runtime_error &e)
{
  wsrep::log_warning() << "Failed to handle error vote: " << e.what();
  return 1;
}

int Wsrep_provider_grp::handle_sync()
try
{
  queue_event(new drain_event(
      [](Wsrep_provider_grp &provider) {
        if (provider.server_state_.state() !=
            wsrep::server_state::s_disconnecting)
        {
          provider.server_state_.on_sync();
        }
      },
      m_last_received));
  return 0;
}
catch (const wsrep::runtime_error &e)
{
  wsrep::log_warning() << "Failed to handle sync: " << e.what();
  return 1;
}

int Wsrep_provider_grp::handle_sst_request(const std::string& method,
                                           const std::string& receive_address)
{
  /* Is this check enough or should we drain first? */
  {
    wsrep::unique_lock<wsrep::mutex> lock(server_state_.mutex());
    if (server_state_.state(lock) != wsrep::server_state::s_synced)
    {
      return 1;
    }
  }
  /* For historical reasons, the request passed to wsrep-lib/wsrep_sst
     methods needs '\0' separator. */
  std::string wsl_request;
  wsl_request+= method;
  wsl_request+= '\0';
  wsl_request+= receive_address;
  return server_state_.start_sst(wsl_request, wsrep::gtid{
        server_state_.current_view().state_id().id(),
          m_last_received}, false /* never bypass with GRP */);
}

wsrep::seqno Wsrep_provider_grp::last_committed() const
{
  return wsrep::seqno{m_last_committed.load(std::memory_order_relaxed)};
}

void Wsrep_provider_grp::queue_event(event *e, bool close)
{
  std::unique_ptr<event> evt(e);
  mysql_mutex_lock(&m_events_mutex);
  m_events.push_back(std::move(evt));
  mysql_cond_signal(&m_events_cond);
  mysql_mutex_unlock(&m_events_mutex);
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::connect(const std::string &cluster_name,
                            const std::string &cluster_url,
                            const std::string & /* Unused */, bool bootstrap)
{
  DBUG_ENTER("Wsrep_provider_grp::connect");
  DBUG_PRINT("grp", ("connect: %s, %s, %d", cluster_name.c_str(),
                     cluster_url.c_str(), bootstrap));

  if (validate_grp_cluster_address(cluster_url))
  {
    DBUG_RETURN(error_provider_failed);
  }

  if (bootstrap && wsrep_schema->clear_on_bootstrap())
  {
    wsrep::log_error() << "Failed to clear wsrep schema on bootstrap";
    DBUG_RETURN(error_provider_failed);
  }

  m_closed= false;
  /* We assume gapless monotonically increasing seqnos from the
     provider. However, leaving the cluster also consumes a seqno
     which is currently not visible for us in leave view. Therefore
     m_last_received must be reset at some point if the node leaves
     and rejoins the cluster without restarting the process. We do the
     reset here in lack of better place. */
  m_last_received= wsrep::seqno::undefined();

  int64_t provider_position= wsrep_get_SE_checkpoint_provider_position();
  wsrep::log_info() << "Connecting to cluster with replication log position "
                    << provider_position;
  m_start_position= provider_position;
  auto cluster_address= make_grp_cluster_address(cluster_url);

  grp_connect_args_t args {
    cluster_name.c_str(),
    wsrep_node_name,
    cluster_address.c_str(),
    server_state_.incoming_address().c_str(),
    { wsrep_gtid_domain_id },
    provider_position,
    bootstrap
  };

  if (m_grp->connect(&args))
  {
    WSREP_ERROR("Failed to connect");
    DBUG_RETURN(error_provider_failed);
  }
  DBUG_RETURN(success);
}

int Wsrep_provider_grp::disconnect()
{
  auto ret= m_grp->disconnect();
  if (!ret)
  {
    mysql_mutex_lock(&m_events_mutex);
    while (!m_closed)
    {
      mysql_cond_wait(&m_events_cond, &m_events_mutex);
    }
    mysql_mutex_unlock(&m_events_mutex);
  }
  /* Sanity checks to catch issues with last committed tracking/waiting
     if they happen. */
  mysql_mutex_lock(&m_last_committed_mutex);
  DBUG_ASSERT(m_last_committed_waiters.empty());
  DBUG_ASSERT(m_last_committed_low.get() == m_last_committed);
  mysql_mutex_unlock(&m_last_committed_mutex);
  return ret;
}

int Wsrep_provider_grp::capabilities() const { return provider_capabilities; }

static void handle_desync(grp_command_handler_context_t *ctx, int error_code,
                          const void *ptr)
{
  DBUG_ASSERT(error_code || ptr);
  auto *provider= reinterpret_cast<Wsrep_provider_grp *>(ctx);
  if (!ptr)
  {
    provider->handle_desync(error_code, 0);
  }
  else
  {
    provider->handle_desync(error_code, *(int *) ptr);
  }
}

void Wsrep_provider_grp::handle_desync(int error_code, int desync_count)
{
  mysql_mutex_lock(&m_desync_mutex);
  m_status.desync_count = desync_count;
  m_desync_state= Desync_state::READY;
  mysql_cond_broadcast(&m_desync_cond);
  mysql_mutex_unlock(&m_desync_mutex);
}

int Wsrep_provider_grp::desync() {
  mysql_mutex_lock(&m_desync_mutex);
  grp_command_handler_t handler{
      ::handle_desync,
      reinterpret_cast<grp_command_handler_context_t *>(this)};

  while (m_desync_state != Desync_state::IDLE)
  {
    mysql_cond_wait(&m_desync_cond, &m_desync_mutex);
  }

  if (m_grp->desync(handler))
  {
    mysql_mutex_unlock(&m_desync_mutex);
    return 1;
  }
  m_desync_state= Desync_state::REQUESTED;

  while (m_desync_state != Desync_state::READY) {
    mysql_cond_wait(&m_desync_cond, &m_desync_mutex);
  }

  m_desync_state= Desync_state::IDLE;
  mysql_cond_broadcast(&m_desync_cond);
  mysql_mutex_unlock(&m_desync_mutex);
  return 0;
}

int Wsrep_provider_grp::resync() {
  mysql_mutex_lock(&m_desync_mutex);
  grp_command_handler_t handler{
      ::handle_desync,
      reinterpret_cast<grp_command_handler_context_t *>(this)};

  while (m_desync_state != Desync_state::IDLE)
  {
    mysql_cond_wait(&m_desync_cond, &m_desync_mutex);
  }

  if (m_grp->resync(handler))
  {
    mysql_mutex_unlock(&m_desync_mutex);
    return 1;
  }
  m_desync_state= Desync_state::REQUESTED;

  while (m_desync_state != Desync_state::READY) {
    mysql_cond_wait(&m_desync_cond, &m_desync_mutex);
  }

  m_desync_state= Desync_state::IDLE;
  mysql_cond_broadcast(&m_desync_cond);
  mysql_mutex_unlock(&m_desync_mutex);
  return 0;
}

static void handle_pause(grp_command_handler_context_t *ctx,
                         int error_code, const void *)
{
  auto *provider= reinterpret_cast<Wsrep_provider_grp*>(ctx);
  provider->handle_pause(error_code);
}

void Wsrep_provider_grp::handle_pause(int error_code)
{
  queue_event(new drain_event([error_code, this](Wsrep_provider_grp &self) {
    mysql_mutex_lock(&m_pause_mutex);
    DBUG_ASSERT(m_pause_seqno.is_undefined());
    DBUG_ASSERT(m_pause_state == Pause_state::REQUESTED);
    wait_for_committed_slow(m_last_received);
    if (!error_code)
    {
      /* Event queue must have been drained. */
      DBUG_ASSERT(m_events.empty());
      m_pause_seqno=
        wsrep::seqno{m_last_committed.load(std::memory_order_relaxed)};
    }
    m_pause_state= Pause_state::READY;
    mysql_cond_signal(&m_pause_cond);
    mysql_mutex_unlock(&m_pause_mutex);
  }, m_last_received));
}

wsrep::seqno Wsrep_provider_grp::pause()
{
  grp_command_handler_t handler{
      ::handle_pause, reinterpret_cast<grp_command_handler_context_t *>(this)};

  mysql_mutex_lock(&m_pause_mutex);
  /* We currently handle one pauser at the time. */
  DBUG_ASSERT(m_pause_state == Pause_state::IDLE);
  if (m_grp->pause(handler))
  {
    mysql_mutex_unlock(&m_pause_mutex);
    return {};
  }
  m_pause_state= Pause_state::REQUESTED;
  while (m_pause_state != Pause_state::READY)
  {
    mysql_cond_wait(&m_pause_cond, &m_pause_mutex);
  }
  auto ret= m_pause_seqno;
  mysql_mutex_unlock(&m_pause_mutex);
  return ret;
}

static void handle_resume(grp_command_handler_context_t *ctx, int error_code, const void *)
{
  auto *provider= reinterpret_cast<Wsrep_provider_grp*>(ctx);
  provider->handle_resume(error_code);
}

void Wsrep_provider_grp::handle_resume(int error_code)
{
  if (error_code)
  {
    wsrep::log_warning() << "Failed to resume provider: " << error_code;
  }
  mysql_mutex_lock(&m_pause_mutex);
  m_pause_seqno= wsrep::seqno::undefined();
  m_pause_state= Pause_state::IDLE;
  mysql_cond_signal(&m_pause_cond);
  mysql_mutex_unlock(&m_pause_mutex);
}

int Wsrep_provider_grp::resume()
{
  mysql_mutex_lock(&m_pause_mutex);
  DBUG_ASSERT(m_pause_state == Pause_state::READY);
  grp_command_handler_t handler{
      ::handle_resume,
      reinterpret_cast<grp_command_handler_context_t *>(this)};
  if (m_grp->resume(handler))
  {
    mysql_mutex_unlock(&m_pause_mutex);
    return 1;
  }
  while (m_pause_state != Pause_state::IDLE)
  {
    mysql_cond_wait(&m_pause_cond, &m_pause_mutex);
  }
  DBUG_ASSERT(m_pause_seqno.is_undefined());
  mysql_mutex_unlock(&m_pause_mutex);
  return 0;
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::run_applier(wsrep::high_priority_service *applier_ctx)
{
  // Atomic for sanity check at the end of the method
  static std::atomic<int> n_appliers;
  ++n_appliers;
  while (true)
  {
    mysql_mutex_lock(&m_events_mutex);
    if (m_closed)
    {
      --n_appliers;
      mysql_mutex_unlock(&m_events_mutex);
      break;
    }
    if (m_events.empty() || m_isolated_in_progress)
    {
      thd_proc_info(current_thd, "wsrep applier idle");
      mysql_cond_wait(&m_events_cond, &m_events_mutex);
    }
    /* We need to check once more in case of spurious wakeups. */
    if (m_events.empty() || m_isolated_in_progress)
    {
      mysql_mutex_unlock(&m_events_mutex);
      continue;
    }

    auto evt= std::move(m_events.front());
    m_events.pop_front();

    if (evt->is_certified())
    {
      auto *cert_evt= static_cast<certified_event *>(evt.get());
      // Register the applying transaction while being in total order of
      // the events queue.
      auto *thd= cert_evt->origin_thd();
      if (!thd)
      {
        // This is a remote event, use current applier thread.
        thd= current_thd;
      }
    }

    /* Set global isolated flag for isolated events to pause event
       queue processing until this event is applied. */
    DBUG_ASSERT(!m_isolated_in_progress);
    if (evt->is_isolated())
    {
      m_isolated_in_progress= true;
    }

    if (!m_events.empty() && !m_isolated_in_progress)
    {
      mysql_cond_signal(&m_events_cond);
    }
    mysql_mutex_unlock(&m_events_mutex);

    /* TODO: Is this in a right place? In Galera stopping in local
     * monitor blocks all following threads. */
    remote_sync_wait(current_thd, "local_monitor_slave_enter_sync");

    // TODO(denis): Handle apply error.
    evt->apply(*this, applier_ctx);

    bool broadcast= false, bail_out= false;
    mysql_mutex_lock(&m_events_mutex);

    if (evt->is_isolated())
    {
      DBUG_ASSERT(m_isolated_in_progress);
      m_isolated_in_progress= false;
      broadcast= true;
    }

    if (evt->is_final())
    {
      m_closed= true;
      broadcast= true;
      bail_out= true; /* Last event in the queue */
    }
    else if (applier_ctx && applier_ctx->must_exit())
    {
      /* Sanity check against a race somewhere else in code which controls
         number of appliers. This is very similar what Galera has. Emit a
         warning to see if this ever happens with GRP. */
      if (!m_closed && n_appliers.load() == 1)
      {
        WSREP_WARN("Not closing the last applier");
      }
      else
      {
        bail_out= true; /* Applier exit was requested by the server. */
      }
    }

    if (broadcast)
    {
      mysql_cond_broadcast(&m_events_cond);
    }

    if (bail_out)
    {
      --n_appliers;
      mysql_mutex_unlock(&m_events_mutex);
      break;
    }
    mysql_mutex_unlock(&m_events_mutex);
  }
  DBUG_ASSERT(n_appliers.load() >= 0);
  return success;
}

int Wsrep_provider_grp::start_transaction(wsrep::ws_handle &ws_handle)
try
{
  auto write_set= new Wsrep_write_set(m_last_committed.load(),
                                      ws_handle.transaction_id().get());
  ws_handle= {ws_handle.transaction_id(), write_set};
  return 0;
}
catch (const std::bad_alloc &)
{
  wsrep::log_error() << "Failed to allocate memory for a transaction";
  return 1;
}
catch (...)
{
  wsrep::log_error() << "Failed to start a transaction";
  return 1;
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::assign_read_view(wsrep::ws_handle &, const wsrep::gtid *)
{
  return success;
}

int Wsrep_provider_grp::append_key(wsrep::ws_handle &ws_handle,
                                   const wsrep::key &key)
{
  auto *write_set= static_cast<Wsrep_write_set *>(ws_handle.opaque());
  DBUG_ASSERT(write_set);
  write_set->append_key(key);
  return 0;
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::append_data(wsrep::ws_handle &ws_handle,
                                const wsrep::const_buffer &data)
{
  auto *write_set= static_cast<Wsrep_write_set *>(ws_handle.opaque());
  DBUG_ASSERT(write_set);
  write_set->append_data(data);
  return success;
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::certify(wsrep::client_id client,
                            wsrep::ws_handle &ws_handle, int flags,
                            wsrep::ws_meta &ws_meta, const wsrep::provider::seq_cb_t* cb)
{
  DBUG_ENTER("Wsrep_provider_grp::certify");

  auto *write_set= static_cast<Wsrep_write_set *>(ws_handle.opaque());
  DBUG_ASSERT(write_set);

  debug_sync_wait("before_replicate_sync");

  /* XA PREPARE, XA COMMIT and XA ROLLBACK by XID not allowed.
   * XA PREPARE - flag::prepare is set
   * XA COMMIT and XA ROLLBACK - the in/out parameter ws_meta transaction_id
   *   is defined. To see the explanation for this convention, see
   *   https://github.com/MariaDB/galera/blob/e792f3e41056092eda2ed2a27e486b5436798351/galera/src/wsrep_provider.cpp#L571
   *   https://github.com/codership/wsrep-lib/blob/3965d839684190afde1ecdd3825414f7f06917f9/src/transaction.cpp#L1199
  */
  if ((flags & wsrep::provider::flag::prepare) ||
      !ws_meta.transaction_id().is_undefined())
  {
    DBUG_RETURN(status::error_not_implemented);
  }
  /*
   We need to consider two distinct cases when BF aborting (interrupting)
   a transaction:
     - If BF abort happened before replicating the write set, then it's
     fine to mark the transaction as aborted with no further actions.
     This case is reflected with returning CONN_FAIL status code.
     - If BF abort happens after the write set has been replicated, then
     other nodes might have received it and are going to apply it.
     Just aborting the transaction may lead to an inconsistency, so the
     outcome from the certification must be learned, and the write set
     must be applied in case of successful certification.
     At the same time, the transaction cannot keep waiting as it's been
     asked to concede to a high-priority transaction that issued BF abort,
     so the only way to learn the certification outcome is by replaying
     the victim and deciding its fate there.
     To signal this, return BF_ABORT status code.

   The second case corresponds to `notify_sent_and_wait_certified()`
   error return.
   */
  if (write_set->finalize(m_last_committed.load(), flags)) {
    WSREP_DEBUG("Certification was interrupted");
    DBUG_RETURN(error_connection_failed);
  }

  if (m_grp->certify(write_set))
  {
    WSREP_ERROR("Failed to certify for client %llu", client.get());
    DBUG_RETURN(error_provider_failed);
  }

  /* Replication order of write sets has been established, notify the
     caller. */
  if (cb && cb->fn)
  {
    cb->fn(cb->ctx);
  }

  write_set->update_status(m_status);

  if (write_set->notify_sent_and_wait_certified()) {
    WSREP_DEBUG("Certification result wait was interrupted");
    DBUG_RETURN(error_bf_abort);
  }

  debug_sync_wait("apply_monitor_master_enter_sync");
  debug_sync_wait("before_certify_apply_monitor_enter");
  debug_sync_wait("after_certify_apply_monitor_enter");

  const auto status= make_wsrep_cert_status(write_set->cert_result);
  if (status != success)
  {
    if (status == wsrep::provider::error_certification_failed)
    {
      ++m_status.local_cert_failures;
    }
    DBUG_RETURN(status);
  }

  ws_meta= make_wsrep_ws_meta(
      *write_set,
      wsrep::stid{server_state_.id(), ws_handle.transaction_id(), client},
      flags);
  current_thd->wsrep_grp_log_position = write_set->cert_meta.provider_position;

  DBUG_RETURN(success);
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::bf_abort(wsrep::seqno bf_seqno,
                             wsrep::transaction_id /* victim_trx */,
                             wsrep::client_service& victim_ctx,
                             wsrep::seqno &victim_seqno)
{
  /* TODO: Is there any way to do this without downcasting? */
  auto victim_thd_ctx= static_cast<Wsrep_client_service&>(victim_ctx);
  auto *victim_thd= victim_thd_ctx.thd();
  DBUG_ASSERT(victim_thd);
  mysql_mutex_assert_owner(&victim_thd->LOCK_thd_data);

  victim_seqno= victim_thd->wsrep_trx().seqno();

  /* Don't allow aborting of committing transactions which are earlier
     in commit order. If the victim_seqno is still undefined, it has not been
     assigned the seqno yet because it comes later in total order or
     it has failed certification. In both cases the BF abort
     must be allowed.
  */
  auto ret= status::success;
  if (!victim_seqno.is_undefined() &&
      (victim_seqno < bf_seqno && (victim_thd->wsrep_trx().ws_meta().flags() &
                                   wsrep::provider::flag::commit)))
  {
    ret= status::error_not_allowed;
  }
  else
  {
    /* Interrupt certification result wait. */
    auto *write_set= static_cast<Wsrep_write_set *>(
      victim_thd->wsrep_trx().ws_handle().opaque());
    if (write_set)
    {
      write_set->interrupt(bf_seqno);
    }
    wsrep_grp_bf_abort_group_commit(victim_thd);
  }
  debug_sync_wait("abort_trx_end");
  return ret;
}

enum Wsrep_provider_grp::status
    Wsrep_provider_grp::rollback(wsrep::transaction_id id)
{
  /*
    Rollback fragments are sent to cluster without waiting
    for certification result. It is assumed that rollback fragment
    always succeeds in certification. The write set must be handled
    by an applier thread.

    @todo: Certification keys are currently missing for rollback fragments.
    This means that the rollback fragments are unsafe to apply in parallel,
    which may cause applier stall with large SR transactions. Table level
    keys should be added to deduce dependencies and avoid stop-the-world
    on rollback.
  */
  auto last_committed= m_last_committed.load();
  auto write_set= new Wsrep_write_set(last_committed, id.get());
  write_set->finalize(last_committed, flag::rollback | flag::pa_unsafe);

  if (m_grp->certify(write_set))
  {
    delete write_set;
    return error_connection_failed;
  }

  write_set->update_status(m_status);

  /* On successfull call the write set is consumed by applier. */
  return success;
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::commit_order_enter(const wsrep::ws_handle &ws_handle,
                                       const wsrep::ws_meta &ws_meta)
{
  auto *write_set= static_cast<Wsrep_write_set *>(ws_handle.opaque());
  /* We may be executing in different THD context than what is recorded
     in write set. This happens at least when committing SR fragment into
     storage. Steal the wait_for_commit_ptr from original write_set->thd.
 */
  auto *thd= current_thd;
  if (write_set && write_set->thd() && write_set->thd() != thd)
  {
    thd->wait_for_commit_ptr= write_set->thd()->wait_for_commit_ptr;
    write_set->thd()->wait_for_commit_ptr= nullptr;
  }
  DBUG_ASSERT(thd->mysys_var);
  DBUG_ASSERT(thd->wait_for_commit_ptr);

  local_sync_wait(thd, "commit_monitor_master_enter_sync");
  remote_sync_wait(thd, "commit_monitor_slave_enter_sync");
  local_sync_wait(thd, "before_local_commit_monitor_enter");

  class diagnostics_area_guard
  {
  public:
    diagnostics_area_guard(THD *thd)
      : m_thd{thd}
      , m_orig_da{m_thd->get_stmt_da()}
      , m_tmp_da(m_thd->query_id, false, true)
    {
      m_thd->set_stmt_da(&m_tmp_da);
    }
    ~diagnostics_area_guard()
    {
      m_thd->set_stmt_da(m_orig_da);
    }
  private:
    THD *m_thd;
    Diagnostics_area *m_orig_da;
    Diagnostics_area m_tmp_da;
  } guard{thd};

  if (thd->wait_for_prior_commit())
  {
    if (!(ws_meta.flags() & wsrep::provider::flag::commit))
    {
      /* BF aborted fragment storage operation will not be retried,
       * so it is safe to release commit order here. */
      wsrep_grp_unregister_from_group_commit(thd);
    }
    return error_bf_abort;
  }

  /* Wait for committed is required for local transctions to establish
     proper ordering with respect to view events. This wait is done
     after wait_for_prior_commit() because now it is known that none of the
     prior transactions will be blocked by this one, so the wait does not
     need to be interruptible. */

  if (write_set)
  {
    /* There does not seem to be proper synchronization between storage
    * threads which don't write to binlog and client threads which will
    * write to binlog. When binlog is on and the ws_meta does not have
    * commit flag, the commit is into fragment storage. Make the
    * commit wait for all the prior commits to avoid out-of-order commit.
    * This would probably be better to be fixed elsewhere though...
    * The test galera.mdev_18730 can be used to check whether the problem
    * still exists and if this wait could always be done for depends_on
    * seqno. */
    const auto wait_for_seqno=
        wsrep_emulate_bin_log ||
                (ws_meta.flags() & wsrep::provider::flag::commit)
            ? wsrep::seqno{write_set->cert_meta.depends_on}
            : ws_meta.seqno().prev();
    wait_for_committed(wait_for_seqno);
  }

  return success;
}

__attribute__((warn_unused_result))
static int check_apply_error(st_mysql_galera_replication &grp,
                             Wsrep_apply_error& ae)
{
  do
  {
    int ret= grp.error_on_apply(&ae);
    if (ret)
    {
      wsrep::log_error() << "error_on_apply('"
			 << static_cast<const char *>(ae.data.data)
			 << "[" << ae.data.size << "]'("
			 << ae.seqno << "/" << ae.position << ")"
			 << "failed: " << ret;
      return WSREP_CONN_FAIL;
    }
    ae.wait_result();
    wsrep::log_info() << "Error voting verdict: " << ae.result();
    assert(ae.result() != GRP_VOTE_NONE);
  }
  while (ae.result() == GRP_VOTE_CANCELED);

  switch (ae.result())
  {
  case GRP_VOTE_PASSED: // just continue as normal
    wsrep::log_info() << "Applying error " << ae
                      << " is consistent with the group. Continue.";
    return WSREP_OK;
  case GRP_VOTE_REJECTED:
    wsrep::log_error() << "Applying error " << ae
                       << " is inconsistent with the group. Node failure.";
    return WSREP_NODE_FAIL;
  default:
    wsrep::log_error() << "Fatal runtime error: unexpected vote result: "
                       << ae.result();
    return WSREP_FATAL;
  }
}

__attribute__((warn_unused_result))
static int unregister_from_group_commit_with_error(THD* const thd,
                                                   st_mysql_galera_replication &grp,
                                                   const wsrep::mutable_buffer &err,
                                                   wsrep::seqno seqno,
                                                   int64_t const position)
{
  int ret= WSREP_OK;

  if (err.size() > 0)
  {
    Wsrep_apply_error ae(err.data(), err.size(), seqno.get(), position);
    ret= check_apply_error(grp, ae);
  }

  // TODO:shall we still unregister from group commit if ret != 0?
  wsrep_grp_unregister_from_group_commit(thd);

  return ret;
}

int Wsrep_provider_grp::commit_order_leave(const wsrep::ws_handle &ws_handle,
                                           const wsrep::ws_meta &ws_meta,
                                           const wsrep::mutable_buffer &err)
{
  auto *thd= current_thd;
  DBUG_ASSERT(thd->mysys_var);
  DBUG_ASSERT(thd->wait_for_commit_ptr);
  auto ret= unregister_from_group_commit_with_error(
      thd, *m_grp, err, ws_meta.seqno(), thd->wsrep_grp_log_position);
  DBUG_ASSERT(!thd->wait_for_commit_ptr);
  return ret;
}

void Wsrep_provider_grp::update_last_committed(wsrep::seqno seqno,
                                               int64_t position)
{
  if (!seqno.is_undefined())
  {
    auto last_committed= m_last_committed.load(std::memory_order_relaxed);
    while (last_committed < seqno.get())
    {
      if (m_last_committed.compare_exchange_weak(last_committed, seqno.get(),
                                                 std::memory_order_seq_cst,
                                                 std::memory_order_relaxed))
      {
        break;
      }
    }

    wakeup_waiters(seqno);
  }
  m_grp->release_committed(position);
}

struct Wsrep_provider_grp::waiters_entry
{
  bool committed;
  mysql_cond_t cond;
  waiters_entry() : committed{false}
  {
#ifdef HAVE_PSI_INTERFACE
    static PSI_cond_key key_grp_last_committed_cond;
#endif /* HAVE_PSI_INTERFACE */
    mysql_cond_init(key_grp_last_committed_cond, &cond, nullptr);
  }
  ~waiters_entry()
  {
    DBUG_ASSERT(committed);
    mysql_cond_destroy(&cond);
  }
};

void Wsrep_provider_grp::wakeup_waiters(wsrep::seqno seqno)
{
  mysql_mutex_lock(&m_last_committed_mutex);
  DBUG_ASSERT(m_last_committed_low < seqno);

  /* We are waking up in order and there are no waiters. */
  if (seqno == m_last_committed_low + 1 && m_last_committed_waiters.empty())
  {
    m_last_committed_low= seqno;
    mysql_mutex_unlock(&m_last_committed_mutex);
    return;
  }

  /*
    Mark self committed.

    TODO: Find out if it is possible to avoid allocating waiters
    entry for every pass.
   */
  auto own_i= m_last_committed_waiters.find(seqno);
  if (own_i == m_last_committed_waiters.end())
  {
    auto entry= std::make_shared<waiters_entry>();
    own_i= m_last_committed_waiters.emplace(seqno, entry).first;
  }
  own_i->second->committed= true;

  /* Wake up continuous committed range. */
  auto i = m_last_committed_waiters.begin();
  while (i != m_last_committed_waiters.end())
  {
    const auto cur_seqno= i->first;
    const auto committed= i->second->committed;

    /* Break loop if we encounter an uncommitted entry or if the
       range is not continuous from m_last_committed_low. */
    if (!committed || cur_seqno != m_last_committed_low + 1)
    {
      break;
    }

    m_last_committed_low= cur_seqno;
    auto entry= i->second;
    m_last_committed_waiters.erase(i++);
    mysql_cond_broadcast(&entry->cond);
  }

  mysql_mutex_unlock(&m_last_committed_mutex);
}

int Wsrep_provider_grp::wait_for_committed(wsrep::seqno seqno,
                                           THD *thd) const
{
  if (m_last_committed.load(std::memory_order_relaxed) < seqno.get())
  {
    return wait_for_committed_slow(seqno, thd);
  }
  return 0;
}

static int wait_for_sync(THD *thd, mysql_mutex_t *mutex, mysql_cond_t *cond,
                         const std::function<bool()> &stop_waiting,
                         double seconds)
{
  mysql_mutex_assert_owner(mutex);
  struct timespec wait_until;
  set_timespec(wait_until, seconds);
  int err= 0;
  /* TODO: Handle query/connection kill. */
  while (err == 0 && !stop_waiting())
  {
    err= mysql_cond_timedwait(cond, mutex, &wait_until);
  }
  return err;
}

int Wsrep_provider_grp::wait_for_committed_slow(wsrep::seqno seqno,
                                                THD *thd) const
{
  DBUG_ASSERT(!seqno.is_undefined());

  mysql_mutex_lock(&m_last_committed_mutex);
  /* Already continuous range committed up to seqno. */
  if (m_last_committed_low.get() >= seqno.get())
  {
    mysql_mutex_unlock(&m_last_committed_mutex);
    return 0;
  }
  auto i= m_last_committed_waiters.find(seqno);
  if (i == m_last_committed_waiters.end())
  {
    auto entry= std::make_shared<waiters_entry>();
    i= m_last_committed_waiters.emplace(seqno, entry).first;
  }
  auto entry= i->second;
  int err= 0;
  ++m_status.apply_waits;
  if (thd)
  {
    /* We are called from sync wait. */
    err= wait_for_sync(thd, &m_last_committed_mutex, &entry->cond,
                       [&entry]() { return entry->committed; },
                       thd->variables.wsrep_sync_wait_timeout);
  }
  else
  {
    while (!entry->committed)
    {
      err= mysql_cond_wait(&entry->cond, &m_last_committed_mutex);
    }
  }
  mysql_mutex_unlock(&m_last_committed_mutex);
  return err;
}

int Wsrep_provider_grp::release(wsrep::ws_handle &ws_handle)
{
  auto *write_set= static_cast<Wsrep_write_set *>(ws_handle.opaque());
  if (!write_set)
  {
    DBUG_ASSERT(!current_thd->wait_for_commit_ptr);
    return 0;
  }

  bool is_sr_next_fragment= false;
  if (write_set->kind == GRP_KIND_TRX_START ||
      write_set->kind == GRP_KIND_TRX_FRAGMENT)
  {
    is_sr_next_fragment= true;
  }

  if (!std::move(*write_set).conflict_seqno().is_undefined())
  {
    /*
      Wait for an aborted non-committing SR fragment to certify so
      that its write set object is not accessed concurrently.
      Single-shot and SR committing write sets go through replay()
      and wait there. */
    if (is_sr_next_fragment)
    {
      write_set->wait_certified_if_sent();
    }

    /* Last non-committing fragment of aborted SR which write set has been
       sent will be scheduled for commit as well as replayed write sets. */
    if (current_thd->wait_for_commit_ptr) {
      current_thd->wakeup_subsequent_commits(0);
      wsrep_grp_unregister_from_group_commit(current_thd);
    }
  }
  else
  {
    /* wait_for_commit_ptr may be not null in case the transaction is retried
       through async replication retry logic.
       It sets up its own value for wait_for_commit_ptr and never resets it
       until done. */
    DBUG_ASSERT(current_thd->wait_for_commit_ptr != &current_thd->wsrep_wfc);
  }

  if (write_set->cert_result == GRP_CERT_SUCCESS)
  {
    update_last_committed(make_wsrep_seqno(write_set->cert_meta.seqno),
                          write_set->cert_meta.provider_position);
    current_thd->wsrep_grp_log_position = 0;
  }

  if (write_set->cert_result == GRP_CERT_SUCCESS && is_sr_next_fragment)
  {
    write_set->reset_for_next_fragment();
  }
  else
  {
    delete write_set;
    ws_handle= wsrep::ws_handle{};
  }
  return 0;
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::replay(const wsrep::ws_handle &ws_handle,
                           wsrep::high_priority_service *applier_ctx)
{
  auto ws= static_cast<Wsrep_write_set*>(ws_handle.opaque());
  ws->wait_certified();
  if (ws->cert_result != GRP_CERT_SUCCESS) {
    return error_certification_failed;
  }

  /* Check the conflicting seqno from the original write set. */
  DBUG_ASSERT(!ws->conflict_seqno().is_undefined());

  ++m_status.local_replays;

  /*
    We have here two THD objects to care about:
    - thd is the original THD which was BF aborted
    - current_thd is the THD which is executing replay
   */
  auto seqno= wsrep::seqno{ws->cert_meta.seqno};
  DBUG_ASSERT(!seqno.is_undefined());
  wait_for_committed_slow(seqno.prev());

  auto thd= ws->thd();
  wsrep::ws_meta ws_meta= make_wsrep_ws_meta(
      *ws, wsrep::stid{server_state_.id(), ws_handle.transaction_id(),
                      thd->wsrep_cs().id()}, thd->wsrep_trx().flags());

  thd->wsrep_wfc.wakeup_error= 0;
  /*
    Replaying in current_thd context
   */
  Grp_provider_position_guard provider_position_guard{
    current_thd, ws->cert_meta.provider_position};

  current_thd->wait_for_commit_ptr= &thd->wsrep_wfc;
  applier_ctx->start_transaction(ws_handle, ws_meta);

  wsrep::mutable_buffer unused;
  applier_ctx->apply_write_set(
      ws_meta, wsrep::const_buffer(ws->data.data, ws->data.size), unused);

  applier_ctx->commit(ws_handle, ws_meta);
  return success;
}

enum Wsrep_provider_grp::status Wsrep_provider_grp::enter_toi(
    wsrep::client_id client, const wsrep::key_array &keys,
    const wsrep::const_buffer &buffer, wsrep::ws_meta &ws_meta, int flags)
{
  DBUG_ENTER("Wsrep_provider_grp::enter_toi");

  // For TOI operations read view is equal to last committed.
  const auto last_committed= m_last_committed.load();
  Wsrep_write_set write_set{make_grp_const_buf(buffer), last_committed,
                            wsrep::transaction_id{}.get()};
  std::for_each(keys.begin(), keys.end(),
                [&write_set](const wsrep::key &key) {
                  write_set.append_key(key);
                });
  write_set.finalize(last_committed, flags | wsrep::provider::flag::isolation);

  const auto ret= m_grp->certify(&write_set);
  if (ret)
  {
    WSREP_ERROR("Failed to enter TOI for client %llu", client.get());
    DBUG_RETURN(error_provider_failed);
  }

  write_set.update_status(m_status);

  write_set.notify_sent_and_wait_certified();
  const auto status= make_wsrep_cert_status(write_set.cert_result);
  if (status != success)
  {
    WSREP_ERROR("Certification result not success for TOI: %d", status);
    DBUG_RETURN(status);
  }

  DBUG_ASSERT(current_thd->wait_for_commit_ptr == &current_thd->wsrep_wfc);

  /*
    We need something stronger than current_thd->wait_for_prior_commit().
    DMLs may release commit order as soon as they have been committed in
    memory or entered binlog commit order queue. This happens way before
    MDL locks are released. Wait for depends_on to commit.
  */
  wait_for_committed_slow(wsrep::seqno{write_set.cert_meta.depends_on});

  if (current_thd->wait_for_prior_commit())
  {
    // This should never happen for TOI operations.
    WSREP_ERROR("Failed to wait for prior commit in TOI for client %llu",
                client.get());
    DBUG_RETURN(error_fatal);
  }
  /* We are now fully isolated. Unset wait_for_commit_ptr to avoid
     assertions elsewhere in the code (mysql_admin_table() at least).
     This will be restored in leave_toi() before unregistering from
     group commit. */
  current_thd->wait_for_commit_ptr= nullptr;

  ws_meta= make_wsrep_ws_meta(
      write_set,
      wsrep::stid{server_state_.id(), wsrep::transaction_id{}, client},
      flags);
  current_thd->wsrep_grp_log_position = write_set.cert_meta.provider_position;
  DBUG_RETURN(success);
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::leave_toi(wsrep::client_id, const wsrep::ws_meta &ws_meta,
                              const wsrep::mutable_buffer &err)
{
  DBUG_ENTER("Wsrep_provider_grp::leave_toi");

  // TODO(denis): Send the write set with the status of the TOI operation.

  DBUG_ASSERT(ws_meta.seqno().get() > 0);
  DBUG_ASSERT(ws_meta.seqno().get() > m_last_committed);
  int64_t const position= current_thd->wsrep_grp_log_position;
  current_thd->wsrep_grp_log_position= 0;
  current_thd->wait_for_commit_ptr= &current_thd->wsrep_wfc;
  auto ret= unregister_from_group_commit_with_error(current_thd, *m_grp, err, ws_meta.seqno(), position);
  current_thd->wait_for_commit_ptr= nullptr;
  update_last_committed(ws_meta.seqno(), position);

  DBUG_RETURN(0 == ret ? success : error_provider_failed);
}

#ifdef HAVE_PSI_INTERFACE
static PSI_thread_key KEY_grp_causal_read_mutex;
static PSI_cond_key KEY_grp_causal_read_cond;
#endif /* HAVE_PSI_INTERFACE */
class Wsrep_provider_grp::Causal_read_handler_context
{
public:
  Causal_read_handler_context(const Wsrep_provider_grp& provider)
    : m_provider{provider}
    , m_state{State::WAITING}
    , m_count{2}
    , m_error_code{0}
    , m_gtid{}
  {
    mysql_mutex_init(KEY_grp_causal_read_mutex, &m_mutex, MY_MUTEX_INIT_FAST);
    mysql_cond_init(KEY_grp_causal_read_cond, &m_cond, nullptr);
  }

  ~Causal_read_handler_context()
  {
    mysql_mutex_destroy(&m_mutex);
    mysql_cond_destroy(&m_cond);
  }

  void signal_ready(int error_code)
  {
    mysql_mutex_lock(&m_mutex);
    if (m_state == State::WAITING)
    {
      m_state= State::READY;
      m_error_code= error_code;
      if (!error_code)
      {
        m_gtid= wsrep::gtid{
          m_provider.server_state_.current_view().state_id().id(),
          m_provider.m_last_received
        };
      }
      else
      {
        WSREP_DEBUG("Causal read handler context, signal ready error code: %d",
                    error_code);
      }
      mysql_cond_signal(&m_cond);
    }
    mysql_mutex_unlock(&m_mutex);
    unref();
  }

  wsrep::gtid wait_ready(THD *thd, double seconds)
  {
    wsrep::gtid ret{};
    mysql_mutex_lock(&m_mutex);
    int err= wait_for_sync(
        thd, &m_mutex, &m_cond, [this]() { return m_state != State::WAITING; },
        seconds);
    if (!(err || m_error_code))
    {
      ret= m_gtid;
    }
    mysql_mutex_unlock(&m_mutex);
    return ret;
  }

  bool wait_canceled() const
  {
    return m_error_code == ECANCELED;
  }

  void unref()
  {
    if (--m_count == 0)
    {
      delete this;
    }
  }
private:
  const Wsrep_provider_grp& m_provider;
  enum class State
  {
    WAITING,
    READY,
    ABANDONED
  } m_state;
  std::atomic<int> m_count;
  int m_error_code;
  wsrep::gtid m_gtid;
  mysql_mutex_t m_mutex;
  mysql_cond_t m_cond;
};

static void causal_read_handler(grp_command_handler_context_t *ctx,
                                int error_code, const void *)
{
  auto *read_handler=
      reinterpret_cast<Wsrep_provider_grp::Causal_read_handler_context *>(ctx);
  read_handler->signal_ready(error_code);
}

std::pair<wsrep::gtid, enum Wsrep_provider_grp::status>
Wsrep_provider_grp::causal_read(
    int /* Ignored, we use timeout from thd session variables*/) const
{
  DBUG_ENTER("Wsrep_provider_grp::causal_read");
  const auto wait_started= std::chrono::steady_clock::now();
  const double seconds_to_wait= current_thd->variables.wsrep_sync_wait_timeout;
  double remaining= seconds_to_wait;

  do
  {
    auto ctx= new Causal_read_handler_context(*this);
    struct Causal_read_handler_context_deleter
    {
      void operator()(Causal_read_handler_context *ctx) const { ctx->unref(); }
    };
    std::unique_ptr<Causal_read_handler_context,
                    Causal_read_handler_context_deleter>
        ctx_deleter{ctx};

    grp_command_handler_t handler{
        ::causal_read_handler,
        reinterpret_cast<grp_command_handler_context_t *>(ctx)};

    if (m_grp->sync_wait(handler))
    {
      /* Context didn't reach execution stage in service thread,
         safe to delete. */
      delete ctx;
      DBUG_RETURN(
          std::make_pair(wsrep::gtid::undefined(), error_connection_failed));
    }

    /* Note that wait_ready() deletes the ctx object automatically if the
       reference count drops to zero. Do not try to access ctx after this
       call. */
    const auto gtid= ctx->wait_ready(current_thd, remaining);
    if (!gtid.is_undefined())
    {
      /* Plugin guarantees only that the handler is called after the
         causality is reached with respect to replication position. We must
         do additional wait here to ensure that the operations preceding the
         gtid position have also been committed. */
      int err= wait_for_committed(gtid.seqno(), current_thd);
      if (!err)
      {
        DBUG_RETURN(std::make_pair(gtid, success));
      }
      else
      {
        DBUG_RETURN(
            std::make_pair(wsrep::gtid::undefined(), error_connection_failed));
      }
    }
    else if (ctx->wait_canceled())
    {
      DBUG_RETURN(
          std::make_pair(wsrep::gtid::undefined(), error_connection_failed));
    }

    /* Undefined gtid means that the sync wait in plugin failed, maybe
     * due to non prim. Retry as long as there is time left. */
    remaining=
        seconds_to_wait - std::chrono::duration<double>(
                              std::chrono::steady_clock::now() - wait_started)
                              .count();
    if (remaining > 0)
    {
      std::this_thread::sleep_for(
        std::min(std::chrono::duration<double>(remaining),
                 std::chrono::duration<double>(0.5)));
      WSREP_DEBUG("Retrying causal wait, remaining %f", remaining);
    }
  } while (remaining > 0);

  DBUG_RETURN(std::make_pair(wsrep::gtid::undefined(), error_connection_failed));
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::wait_for_gtid(const wsrep::gtid &gtid, int timeout) const
{
  return wait_for_committed(gtid.seqno()) == 0 ? success
                                               : error_connection_failed;
}

wsrep::gtid Wsrep_provider_grp::last_committed_gtid() const { return {}; }

enum Wsrep_provider_grp::status
Wsrep_provider_grp::sst_sent(const wsrep::gtid &gtid, int err)
{
  if (m_grp->sst_sent(gtid.seqno().get(), err))
  {
    wsrep::log_warning() << "Got an error when reporting SST sent to GRP";
  }
  return success;
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::sst_received(const wsrep::gtid &, int)
{
  return success;
}

enum Wsrep_provider_grp::status
Wsrep_provider_grp::enc_set_key(const wsrep::const_buffer &key)
{
  return success;
}


/* Get galera state enumeration. */
static int get_galera_state(const wsrep::server_state& server_state, bool desynced)
{
  // Galera-compatible behavior.
  if (desynced)
  {
    return WSREP_MEMBER_DONOR;
  }

  using ws = wsrep::server_state;
  switch (server_state.state())
  {
  case ws::state::s_disconnected:
  case ws::state::s_disconnecting:
  case ws::state::s_initializing:
  case ws::state::s_initialized:
  case ws::state::s_connected:
    return WSREP_MEMBER_UNDEFINED;
  case ws::state::s_joiner:
    return WSREP_MEMBER_JOINER;
  case ws::state::s_joined:
    return WSREP_MEMBER_JOINED;
  case ws::state::s_donor:
    return WSREP_MEMBER_DONOR;
  case ws::state::s_synced:
    return WSREP_MEMBER_SYNCED;
  }
  return WSREP_MEMBER_ERROR;
}

/* Return local_state_comment string compatible with Galera. */
static const char *
get_galera_state_comment(const wsrep::server_state &server_state, bool desynced)
{
  // Galera-compatible behavior.
  if (desynced)
  {
    return "Donor/Desynced";
  }

  using ws= wsrep::server_state;
  switch (server_state.state())
  {
  case ws::state::s_disconnected:
    return "Disconnected";
  case ws::state::s_disconnecting:
    return "Disconnecting";
  case ws::state::s_initializing:
  case ws::state::s_initialized:
  case ws::state::s_connected:
    return "Initialized";
  case ws::state::s_joiner:
    return "Joining";
  case ws::state::s_joined:
    return "Joined";
  case ws::state::s_donor:
    return "Donor/Desynced";
  case ws::state::s_synced:
    return "Synced";
  }
  return "Unknown";
}

std::vector<Wsrep_provider_grp::status_variable>
Wsrep_provider_grp::status() const
{
  std::vector<status_variable> ret;
#if defined(ENABLED_DEBUG_SYNC)
  // Variable is commented out for now. Many galera suite tests which
  // set sync points also do some kind of BF aborting, which is not
  // properly handled yet. Re-enable when BF aborts are functional.
  // (void)debug_sync_waiters();
  ret.push_back(
    status_variable{"debug_sync_waiters", debug_sync_waiters()});
#endif /* ENABLED_DEBUG_SYNC */
  ret.push_back(status_variable{
      "desync_count",
        std::to_string(m_status.desync_count.load(std::memory_order_relaxed))});
  ret.push_back(status_variable{
      "last_committed",
      std::to_string(m_last_committed.load(std::memory_order_relaxed))});
  ret.push_back(status_variable{
      "local_state", std::to_string(get_galera_state(server_state_,
                     m_status.desync_count.load(std::memory_order_relaxed)))});
  ret.push_back(status_variable{"local_state_comment",
      get_galera_state_comment(server_state_,
                     m_status.desync_count.load(std::memory_order_relaxed))});
  ret.push_back(status_variable{
      "local_cert_failures", std::to_string(m_status.local_cert_failures.load(
                                 std::memory_order_relaxed))});
  ret.push_back(status_variable{
      "local_replays",
      std::to_string(m_status.local_replays.load(std::memory_order_relaxed))});
  ret.push_back(status_variable{
      "replicated",
      std::to_string(m_status.replicated.load(std::memory_order_relaxed))});
  ret.push_back(status_variable{
      "received",
      std::to_string(m_status.received.load(std::memory_order_relaxed))});
  ret.push_back(status_variable{"received_bytes",
                                std::to_string(m_status.received_bytes.load(
                                    std::memory_order_relaxed))});
  ret.push_back(status_variable{
      "repl_keys",
      std::to_string(m_status.repl_keys.load(std::memory_order_relaxed))});
  ret.push_back(status_variable{"repl_keys_bytes",
                                std::to_string(m_status.repl_keys_bytes.load(
                                    std::memory_order_relaxed))});
  ret.push_back(status_variable{"repl_data_bytes",
                                std::to_string(m_status.repl_data_bytes.load(
                                    std::memory_order_relaxed))});
  ret.push_back(status_variable{
      "apply_waits",
      std::to_string(m_status.apply_waits.load(std::memory_order_relaxed))});

  double cert_deps_distance= 0;
  double cert_interval= 0;
  const auto cert_total= m_status.cert_total.load(std::memory_order_relaxed);
  if (cert_total)
  {
    cert_deps_distance=
        double(m_status.cert_deps_distance.load(std::memory_order_relaxed)) /
               cert_total;
    cert_interval=
        double(m_status.cert_interval.load(std::memory_order_relaxed)) /
               cert_total;
  }
  ret.push_back(status_variable{
      "cert_deps_distance", std::to_string(cert_deps_distance)});
  ret.push_back(status_variable{
      "cert_interval", std::to_string(cert_interval)});

  std::ostringstream state_uuid_os;
  state_uuid_os << server_state_.current_view().state_id().id();
  ret.push_back(status_variable{"local_state_uuid", state_uuid_os.str()});
  std::ostringstream id_os;
  id_os << server_state_.id();
  ret.push_back(status_variable{"node_id", id_os.str()});
  ret.push_back(
      status_variable{"incoming_addresses", compose_incoming_addresses()});
  return ret;
}

void Wsrep_provider_grp::reset_status() {}

std::string Wsrep_provider_grp::options() const { return {}; }

enum Wsrep_provider_grp::status Wsrep_provider_grp::set_node_isolation(enum node_isolation mode)
{
  grp_node_isolation_t grp_mode= GRP_NODE_ISOLATION_NONE; // initialize to make compiler happy
  switch (mode)
  {
  case node_isolation::not_isolated:
    grp_mode= GRP_NODE_ISOLATION_NONE;
    break;
  case node_isolation::isolated:
    grp_mode= GRP_NODE_ISOLATION_CONNECTIVITY;
    break;
  case node_isolation::force_disconnect:
    grp_mode= GRP_NODE_ISOLATION_DISCONNECTED;
    break;
  }

  if (m_grp->set_node_isolation(grp_mode))
  {
    return error_provider_failed;
  }

  return success;
}

/*
 * Wsrep provider options Galera compatibility
 *
 * We implement the following options that are widely used in
 * Galera MTR suites:
 * - dbug: Controlling Galera debug sync points
 * - signal: Ditto
 * - gmcast.isolate
 * - pc.ignore_sb: This is ignored, use galera-plugin-node-weight=2 for
 *   two node tests which need to maintain quorum when other node is
 *   killed.
 */
namespace
{

static void isolate(Wsrep_provider_grp* provider, const std::string& val_str)
  try
{
  auto val= std::stoi(val_str);
  auto grp= provider->get_plugin_descriptor();
  switch (val)
  {
  case 0:
    grp->set_node_isolation(GRP_NODE_ISOLATION_NONE);
    break;
  case 1:
    grp->set_node_isolation(GRP_NODE_ISOLATION_CONNECTIVITY);
    break;
  case 2:
    grp->set_node_isolation(GRP_NODE_ISOLATION_DISCONNECTED);
    break;
  default:
    WSREP_ERROR("Unsupported value for node isolation");
    break;
  }
}
catch (...)
{
  WSREP_ERROR("Failed to parse value for node isolation");
}

static std::map<std::string, std::function<void(Wsrep_provider_grp*, const std::string &val)>>
    options_handlers= {
#if defined(ENABLED_DEBUG_SYNC)
  {"dbug", debug_sync_set_dbug},
  {"signal", debug_sync_signal},
#endif /* ENABLED_DEBUG_SYNC */
  /* Ignored, use galera-plugin-node-weight */
  {"pc.ignore_sb", [](Wsrep_provider_grp*, const std::string&) {}},
  /* Used by MTR tests for isolating the node from the cluster. */
  {"gmcast.isolate", isolate}
};

} // namespace

enum Wsrep_provider_grp::status
Wsrep_provider_grp::options(const std::string &str)
{
  /* Technically we should check escaping, but the options we
     support are rather limited set and don't need escaping. */
  auto params= split(str, ';');

  for (const auto& param : params)
  {
    /* Form key=val=other not handled, but this is enough for our
       limited purposes. */
    const auto& kv= split(param, '=');

    if (kv.empty())
    {
      /* TODO: Check correct return code */
      return status::success;
    }

    auto handler_i= options_handlers.find(kv[0]);
    if (handler_i == options_handlers.end())
    {
      /* TODO: Check correct return code */
      return status::error_provider_failed;
    }

    /* We need to handle empty values for dbug= */
    if (kv.size() == 1)
    {
      handler_i->second(this, "");
    }
    else
    {
      handler_i->second(this, kv[1]);
    }
  }
  return status::success;
}

std::string Wsrep_provider_grp::name() const
{
  return wsrep_get_galera_plugin_name();
}

std::string Wsrep_provider_grp::version() const
{
  return wsrep_get_galera_plugin_version_info();
}

std::string Wsrep_provider_grp::vendor() const
{
  return wsrep_get_galera_plugin_author();
}

void *Wsrep_provider_grp::native() const
{
  return nullptr;
}

wsrep::gtid Wsrep_provider_grp::conflict_gtid(
                    const wsrep::ws_handle& ws_handle) const
{
  auto *write_set= static_cast<Wsrep_write_set *>(ws_handle.opaque());
  return write_set
      ? wsrep::gtid{wsrep::id(), write_set->conflict_seqno()}
      : wsrep::gtid::undefined();
}

void Wsrep_provider_grp::update_incoming_addresses(const wsrep::view &view)
{
  mysql_mutex_lock(&m_status_mutex);
  m_status.incoming_addresses.clear();
  for (const auto &member : view.members())
  {
    m_status.incoming_addresses.push_back(member.incoming());
  }
  mysql_mutex_unlock(&m_status_mutex);
}

std::string Wsrep_provider_grp::compose_incoming_addresses() const
{
  mysql_mutex_lock(&m_status_mutex);
  std::ostringstream incoming_os;
  for (const auto& incoming : m_status.incoming_addresses)
  {
    incoming_os << incoming;
    incoming_os << ",";
  }
  mysql_mutex_unlock(&m_status_mutex);
  auto ret= incoming_os.str();
  if (!ret.empty())
  {
    ret.pop_back();
  }
  return ret;
}


int Wsrep_provider_grp::view_event::apply(
    Wsrep_provider_grp &provider, wsrep::high_priority_service *hps) const
try
{
  auto *applier= static_cast<Wsrep_applier_service*>(hps);
  THD *thd= applier->thd();
  Grp_provider_position_guard provider_position_guard{thd,
    m_provider_position};
  wsrep::log_debug() << "View " << m_view.status() << " seqno "
                     << m_view.view_seqno()
                     << " provider position: " << m_provider_position
                     << " wait seqno: " << m_wait_seqno;

  /* Handle view in isolation. Wait for all preceding committers to finish.
     The certification should ensure that all the following events depend
     on this one. */
  if (!m_wait_seqno.is_undefined())
  {
    provider.wait_for_committed_slow(m_wait_seqno);
  }
  switch (m_view.status())
  {
  case wsrep::view::primary:
    apply_primary(provider, hps);
    break;
  case wsrep::view::non_primary:
    apply_non_primary(provider, hps);
    break;
  case wsrep::view::disconnected:
    apply_disconnected(provider, hps);
    break;
  }
  return 0;
}
catch (...)
{
  wsrep::log_error() << "Applying view failed";
  return 1;
}

void Wsrep_provider_grp::view_event::apply_primary(
    Wsrep_provider_grp &provider, wsrep::high_priority_service *hps) const
{
  /* Begin Galera sync points */
  debug_sync_wait("process_primary_configuration");
  if (provider.server_state_.state() == wsrep::server_state::s_connected &&
      !provider.server_state_.is_initialized())
  {
    /* This is approximately the point where Galera SST request leading to
       IST would be sent. Sync points adapted from Galera
       ReplicatorSMM::request_state_transfer(). */
    debug_sync_wait("before_send_state_request");
    debug_sync_wait("after_shift_to_joining");
  }
  /* End Galera sync points */
  provider.update_incoming_addresses(m_view);
  provider.server_state_.on_view(m_view, hps);
  provider.update_last_committed(m_view.view_seqno(), m_provider_position);
}

void Wsrep_provider_grp::view_event::apply_non_primary(
    Wsrep_provider_grp &provider, wsrep::high_priority_service *hps) const
{
  provider.server_state_.on_view(m_view, hps);
  /* Plugin resets desync counters when node leaves primary component. This is
     not notified separately, so reset the counter here. */
  provider.m_status.desync_count = 0;
  /* If provider position is set, the non-primary view corresponds to some
     configuration change which has unique seqno assigned. */
  if (m_provider_position > 0) {
    auto gtid= wsrep_get_SE_checkpoint<wsrep::gtid>();
    auto server_gtid= wsrep_get_SE_checkpoint<wsrep_server_gtid_t>();
    wsrep_set_SE_checkpoint(gtid, server_gtid, m_provider_position);
    provider.update_last_committed(m_view.view_seqno(), m_provider_position);
  }
}

void Wsrep_provider_grp::view_event::apply_disconnected(
      Wsrep_provider_grp &provider, wsrep::high_priority_service *hps) const
{
  DBUG_ASSERT(m_view.is_final());
  provider.server_state_.on_view(m_view, hps);
  /* Plugin resets desync counters when node leaves primary component. This is
     not notified separately, so reset the counter here. */
  provider.m_status.desync_count = 0;
  /* Sync the final provider position if set. This is needed to mark the
     final configuration change applied in case when the last node leaves
     the cluster. Without this, bootstrapping the node which was the last
     in the previous cluster is not possible. */
  if (m_provider_position > 0)
  {
    auto gtid= wsrep_get_SE_checkpoint<wsrep::gtid>();
    auto server_gtid= wsrep_get_SE_checkpoint<wsrep_server_gtid_t>();
    wsrep_set_SE_checkpoint(gtid, server_gtid, m_provider_position);
  }
  provider.m_sr_write_sets_recovered = false;
}

int Wsrep_provider_grp::drain_event::apply(
    Wsrep_provider_grp &provider, wsrep::high_priority_service *) const
try
{
  /* Todo: We want to wait until all the preceding events have been
     applied before calling m_func, and at the same time block applying
     all the following events until the m_func returns. So far we have
     only one applier, so this kind of works... */
  if (!m_wait_seqno.is_undefined()) {
    provider.wait_for_committed_slow(m_wait_seqno);
  }
  m_func(provider);
  return 0;
}
catch (...)
{
  wsrep::log_error() << "Applying drain event failed";
  return 1;
}

Wsrep_provider_grp::remote_certified_event::remote_certified_event(
    const grp_write_set_t &write_set)
{
  const wsrep::transaction_id trx_id{write_set.transaction_id};
  m_ws_handle= wsrep::ws_handle{trx_id, nullptr};

  m_ws_meta= make_wsrep_ws_meta(
      write_set,
      wsrep::stid{make_wsrep_id(write_set.cert_meta.id), trx_id,
                  wsrep::client_id{write_set.client_id}},
      make_wsrep_flags(write_set.kind));
  m_provider_position = write_set.cert_meta.provider_position;

  m_data.resize(write_set.data.size);
  memcpy(m_data.data(), write_set.data.data, write_set.data.size);
}

int Wsrep_provider_grp::remote_certified_event::apply(
    Wsrep_provider_grp &provider,
    wsrep::high_priority_service *high_priority_service) const
try
{
  DBUG_ASSERT(high_priority_service);
  Wsrep_applier_service *applier=
      static_cast<Wsrep_applier_service *>(high_priority_service);
  applier->thd()->wait_for_commit_ptr= const_cast<wait_for_commit *>(&m_wfc);
  debug_sync_wait("apply_monitor_slave_enter_sync");

  Grp_provider_position_guard provider_position_guard{applier->thd(),
    m_provider_position};

  provider.wait_for_committed_slow(m_ws_meta.depends_on());
  auto ret= high_priority_service->apply(
      m_ws_handle, m_ws_meta,
      wsrep::const_buffer{m_data.data(), m_data.size()});

  /* Must have gone through group commit. */
  DBUG_ASSERT(!applier->thd()->wait_for_commit_ptr);

  provider.update_last_committed(m_ws_meta.seqno(), m_provider_position);

  debug_sync_wait("recv_IST_after_apply_trx");

  return ret;
}
catch (...)
{
  wsrep::log_error() << "Applying apply event failed";
  return 1;
}
