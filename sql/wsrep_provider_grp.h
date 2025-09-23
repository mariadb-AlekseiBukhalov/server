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

#ifndef WSREP_PROVIDER_GRP_H
#define WSREP_PROVIDER_GRP_H

#include <memory>
#include <deque>
#include <vector>

/* wsrep-lib */
#include "wsrep/server_state.hpp"
#include "wsrep/provider.hpp"

#include "my_global.h"
#include "my_pthread.h"
#include "sql_class.h" /* wait_for_commit */
#include "mysql/plugin_galera_replication.h"

struct st_mysql_galera_replication;
typedef struct st_grp_write_set grp_write_set_t;

class Wsrep_provider_grp : public wsrep::provider
{
public:
  static std::unique_ptr<provider>
  make_provider(wsrep::server_state &server_state,
                const wsrep::provider::services& services);

  wsrep::provider::services& services();

  int handle_connected(wsrep::view &&view);
  int handle_view(wsrep::view &&, long long provider_position);
  int handle_write_set(grp_write_set_t *, bool);
  int handle_error_vote(grp_apply_error_t*, grp_vote_result_t, bool);
  int handle_sync();
  int handle_sst_request(const std::string& method, const std::string& address);
  void handle_desync(int error_code, int desync_count);
  void handle_pause(int);
  void handle_resume(int);
  wsrep::seqno last_committed() const;
  class Causal_read_handler_context;
  st_mysql_galera_replication *get_plugin_descriptor()
  {
    return m_grp;
  };

private:
  explicit Wsrep_provider_grp(wsrep::server_state &server_state,
                              const wsrep::provider::services& services,
                              st_mysql_galera_replication *grp);
  ~Wsrep_provider_grp();


  enum status connect(const std::string &cluster_name,
                      const std::string &cluster_url,
                      const std::string &state_donor, bool bootstrap) override;
  int disconnect() override;

  int capabilities() const override;
  int desync() override;
  int resync() override;

  wsrep::seqno pause() override;
  int resume() override;

  enum status run_applier(wsrep::high_priority_service *applier_ctx) override;
  int start_transaction(wsrep::ws_handle &) override;
  enum status assign_read_view(wsrep::ws_handle &,
                               const wsrep::gtid *) override;
  int append_key(wsrep::ws_handle &, const wsrep::key &) override;
  enum status append_data(wsrep::ws_handle &,
                          const wsrep::const_buffer &) override;
  enum status certify(wsrep::client_id, wsrep::ws_handle &, int,
                      wsrep::ws_meta &, const wsrep::provider::seq_cb_t*) override;

  enum status bf_abort(wsrep::seqno bf_seqno, wsrep::transaction_id victim_trx,
                       wsrep::client_service& victim_ctx,
                       wsrep::seqno &victim_seqno) override;
  enum status rollback(wsrep::transaction_id) override;
  enum status commit_order_enter(const wsrep::ws_handle &,
                                 const wsrep::ws_meta &) override;
  __attribute__((warn_unused_result))
  int commit_order_leave(const wsrep::ws_handle &, const wsrep::ws_meta &,
                         const wsrep::mutable_buffer &err) override;
  int release(wsrep::ws_handle &) override;

  enum status replay(const wsrep::ws_handle &ws_handle,
                     wsrep::high_priority_service *applier_ctx) override;

  enum status enter_toi(wsrep::client_id, const wsrep::key_array &keys,
                        const wsrep::const_buffer &buffer,
                        wsrep::ws_meta &ws_meta, int flags) override;
  enum status leave_toi(wsrep::client_id, const wsrep::ws_meta &,
                        const wsrep::mutable_buffer &err) override;

  std::pair<wsrep::gtid, enum status> causal_read(int timeout) const override;
  enum status wait_for_gtid(const wsrep::gtid &, int timeout) const override;

  wsrep::gtid last_committed_gtid() const override;
  enum status sst_sent(const wsrep::gtid &, int) override;
  enum status sst_received(const wsrep::gtid &, int) override;
  enum status enc_set_key(const wsrep::const_buffer &key) override;
  std::vector<status_variable> status() const override;
  void reset_status() override;

  std::string options() const override;
  enum status options(const std::string &) override;

  enum status set_node_isolation(enum node_isolation mode) override;

  std::string name() const override;
  std::string version() const override;
  std::string vendor() const override;
  void *native() const override;

  wsrep::gtid conflict_gtid(const wsrep::ws_handle& ws_handle) const override;

  class event
  {
  public:
    virtual ~event()= default;
    /** Apply the event.
     *
     * @param provider Provider to apply the event to.
     * @param hps High priority service to apply the event with.
     * @return 0 on success, non-zero on error.
     */
    virtual int apply(Wsrep_provider_grp &provider,
                      wsrep::high_priority_service *hps) const= 0;
    /** Return whether the event is certified. */
    virtual bool is_certified() const { return false; }
    /** Return whether the event is isolated. Applying isolated event pauses
     * event queue processing until the event is applied. */
    virtual bool is_isolated() const { return false; }
    /** Return whether the event is final. Final event closes the event queue.
     */
    virtual bool is_final() const { return false; }
  };

  class view_event : public event
  {
  public:
    view_event(wsrep::view &&view, wsrep::seqno wait_seqno,
               long long provider_position, bool is_final)
        : m_view{std::move(view)}, m_wait_seqno{wait_seqno},
          m_provider_position{provider_position}, m_is_final{is_final}
    {
    }
    int apply(Wsrep_provider_grp &,
              wsrep::high_priority_service *) const override;
    bool is_final() const override { return m_is_final; }
    bool is_isolated() const override { return true; }
  private:
    void apply_primary(Wsrep_provider_grp &provider,
                      wsrep::high_priority_service *hps) const;
    void apply_non_primary(Wsrep_provider_grp &provider,
                      wsrep::high_priority_service *hps) const;
    void apply_disconnected(Wsrep_provider_grp &provider,
                           wsrep::high_priority_service *hps) const;
    wsrep::view m_view;
    wsrep::seqno m_wait_seqno;
    long long m_provider_position;
    bool m_is_final;
  };

  /* Helper event to drain all prior events and execute a function. */
  class drain_event : public event
  {
  public:
    /* Drain all prior events and execute a function.
     *
     * @param func Function to execute.
     * @param wait_seqno Sequence number to wait for.
     */
    drain_event(std::function<void(Wsrep_provider_grp &)> func,
                wsrep::seqno wait_seqno)
        : m_func(std::move(func)), m_wait_seqno(wait_seqno)
    {
    }
    int apply(Wsrep_provider_grp &,
              wsrep::high_priority_service *) const override;
    bool is_isolated() const override { return true; }
  private:
    std::function<void(Wsrep_provider_grp &)> m_func;
    wsrep::seqno m_wait_seqno;
  };

  class certified_event : public event
  {
  public:
    bool is_certified() const override { return true; }
    virtual THD *origin_thd() const= 0;
  };

  class remote_certified_event : public certified_event
  {
  public:
    remote_certified_event(const grp_write_set_t &write_set);
    int apply(Wsrep_provider_grp &,
              wsrep::high_priority_service *) const override;
    THD *origin_thd() const override { return nullptr; }
    wait_for_commit *wfc() { return &m_wfc; }
  private:
    wsrep::ws_handle m_ws_handle;
    wsrep::ws_meta m_ws_meta;
    long long m_provider_position;
    std::vector<char> m_data;
    wait_for_commit m_wfc;
  };

  void queue_event(event *, bool close= false);

  /* Update last received. Returns the previous value of m_last_received. */
  wsrep::seqno update_last_received(wsrep::seqno seqno);
  /* Update last committed seqno. */
  void update_last_committed(wsrep::seqno seqno, int64_t position);
  /* Wake up threads which are waiting this seqno to be committed. */
  void wakeup_waiters(wsrep::seqno seqno);
  /* Wait for seqno becoming committed. This should not be used
     for apply order waiting, see wait_for_commit_slow().

     Returns zero on success, non-zero if the wait timed out or
     was interrupted.

     If the thd parameter is passed, the wait times out in
     wsrep_sync_wait_timeout.
  */
  int wait_for_committed(wsrep::seqno seqno, THD *thd = nullptr) const;
  /*
    Waiting for m_last_committed does not suffice in all the cases.
    E.g. in the following scenario with ordered transactions T3 gets past
    this wait for early and may cause BF-BF conflict when grabbing
    MDL. Note that even if the commits are ordered, the rest of the
    processing including MDL release is unordered after binlog
    and storage engine commit.

    - T1 (high prio DML)
    - T2 (high prio DML)
    - T3 (TOI), depends on T1

    T1                      T2                       T3

                   update last committed
                                                wait for T1

    release MDL

    Returns zero on success, non-zero if the wait timed out or
    was interrupted.

    If the thd parameter is passed, the wait times out in
    wsrep_sync_wait_timeout.
  */
  int wait_for_committed_slow(wsrep::seqno seqno, THD *thd = nullptr) const;
  void queue_write_set_for_applying(const grp_write_set_t *);
  void update_incoming_addresses(const wsrep::view&);
  std::string compose_incoming_addresses() const;

  mysql_mutex_t m_events_mutex;
  mysql_cond_t m_events_cond;
  /* Events waiting to be applied. */
  std::deque<std::unique_ptr<event>> m_events;
  /* Isolated event in progress, */
  bool m_isolated_in_progress;
  /* For waiting to become closed. Protected by events mutex. */
  bool m_closed;
  /* Last sequence number received from the replication. This is needed
     for drain bookkeeping. Some events execute without associated
     sequence number, and m_last_received gives current position. */
  wsrep::seqno m_last_received;
  /* Keep track of the last committed sequence number. */
  std::atomic<wsrep::seqno::native_type> m_last_committed;
  /* Synchronization for slow path. See wakeup_waiters() and
     wait_for_committed_slow() */
  /* Lowest seqno which has been marked committed but not yet removed
     from m_last_committed_waiters. The range
     [m_last_committed_low, m_last_committed] was completed out of order. */
  mutable wsrep::seqno m_last_committed_low;
  mutable mysql_mutex_t m_last_committed_mutex;
  struct waiters_entry;
  mutable std::map<wsrep::seqno, std::shared_ptr<waiters_entry>>
      m_last_committed_waiters;

  enum class Desync_state {
    IDLE,
    REQUESTED,
    READY
  } m_desync_state;
  mysql_mutex_t m_desync_mutex;
  mysql_cond_t m_desync_cond;

  /* Context for pausing and resuming the provider */
  wsrep::seqno m_pause_seqno;
  enum class Pause_state {
    IDLE,
    REQUESTED,
    READY
  } m_pause_state;
  mysql_mutex_t m_pause_mutex;
  mysql_cond_t m_pause_cond;

  /* Counters for status variables. */
  mutable mysql_mutex_t m_status_mutex;

public:
  struct Status
  {
    std::atomic<size_t> local_replays{0};
    std::atomic<size_t> local_cert_failures{0};
    std::atomic<size_t> replicated{0};
    std::atomic<size_t> received{0};
    std::atomic<size_t> received_bytes{0};
    std::atomic<size_t> repl_keys{0};
    std::atomic<size_t> repl_keys_bytes{0};
    std::atomic<size_t> repl_data_bytes{0};
    std::atomic<int> desync_count{0};
    std::atomic<size_t> apply_waits{0};
    std::atomic<size_t> cert_total{0};
    std::atomic<int64_t> cert_deps_distance{0};
    std::atomic<int64_t> cert_interval{0};
    std::vector<std::string> incoming_addresses{};
  };
private:
  provider::services m_services;
  mutable Status m_status;
  st_mysql_galera_replication *m_grp;
  bool m_sr_write_sets_recovered{false};
  long long m_start_position{0};
};

/**
   Make GRP style cluster address from cluster_url. This strips
   out gcomm:// prefix.
 */
std::string make_grp_cluster_address(const std::string &cluster_url);

int make_wsrep_flags(grp_ws_kind_t);

Wsrep_provider_grp* get_provider_grp();

#endif // WSREP_PROVIDER_GRP_H
