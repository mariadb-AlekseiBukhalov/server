/* Copyright 2018 Codership Oy <info@codership.com>

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

#ifndef WSREP_HIGH_PRIORITY_SERVICE_H
#define WSREP_HIGH_PRIORITY_SERVICE_H

#include "wsrep/high_priority_service.hpp"
#include "my_global.h"
#include "sql_error.h" /* Diagnostics area */
#include "sql_class.h" /* rpl_group_info */

class THD;
class Relay_log_info;
class Wsrep_server_service;

class Wsrep_high_priority_service :
  public wsrep::high_priority_service,
  public wsrep::high_priority_context
{
public:
  explicit Wsrep_high_priority_service(THD*);
  ~Wsrep_high_priority_service() override;
  int start_transaction(const wsrep::ws_handle&,
                        const wsrep::ws_meta&) override;
  int next_fragment(const wsrep::ws_meta&) override;
  const wsrep::transaction& transaction() const override;
  int adopt_transaction(const wsrep::transaction&) override;
  int apply_write_set(const wsrep::ws_meta&, const wsrep::const_buffer&,
                      wsrep::mutable_buffer&) override = 0;
  int append_fragment_and_commit(const wsrep::ws_handle&,
                                 const wsrep::ws_meta&,
                                 const wsrep::const_buffer&,
                                 const wsrep::xid&) override;
  int remove_fragments(const wsrep::ws_meta&) override;
  int commit(const wsrep::ws_handle&, const wsrep::ws_meta&) override;
  int rollback(const wsrep::ws_handle&, const wsrep::ws_meta&) override;
  int apply_toi(const wsrep::ws_meta&, const wsrep::const_buffer&,
                wsrep::mutable_buffer&) override;
  void store_globals() override;
  void reset_globals() override;
  void switch_execution_context(wsrep::high_priority_service&) override;
  int log_dummy_write_set(const wsrep::ws_handle&,
                          const wsrep::ws_meta&,
                          wsrep::mutable_buffer&) override;
  void adopt_apply_error(wsrep::mutable_buffer&) override;

  virtual bool check_exit_status() const = 0;
  void debug_crash(const char*) override;
protected:
  friend Wsrep_server_service;
  THD* m_thd;
  Relay_log_info*   m_rli;
  rpl_group_info*   m_rgi;
  struct shadow
  {
    ulonglong      option_bits;
    uint           server_status;
    struct st_vio* vio;
    ulong          tx_isolation;
    char*          db;
    size_t         db_length;
    //struct timeval user_time;
    my_hrtime_t user_time;
    longlong       row_count_func;
    bool           wsrep_applier;
  } m_shadow;
};

class Wsrep_applier_service : public Wsrep_high_priority_service
{
public:
  explicit Wsrep_applier_service(THD*);
  ~Wsrep_applier_service() override;
  int apply_write_set(const wsrep::ws_meta&, const wsrep::const_buffer&,
                      wsrep::mutable_buffer&) override;
  int apply_nbo_begin(const wsrep::ws_meta&, const wsrep::const_buffer& data,
                      wsrep::mutable_buffer& err) override;
  void after_apply() override;
  bool is_replaying() const override { return false; }
  bool check_exit_status() const override;
};

class Wsrep_prepared_applier_service : public Wsrep_applier_service
{
public:
  Wsrep_prepared_applier_service(THD* thd, THD* orig_thd, XID* xid)
    : Wsrep_applier_service(thd)
  {
    m_xid.set(xid);
    /* Copy the transactional mdl locks from the original thd
       that is detached (client is disconnecting or
       pseudo_slave_mode = ON). We rely on these locks to avoid
       potential BF aborts from conflicting DDLs. */
    if (m_thd->mdl_context.clone_transactional_locks(&orig_thd->mdl_context))
    {
      DBUG_ASSERT(0);
    }
  }
  ~Wsrep_prepared_applier_service() override = default;
  int start_transaction(const wsrep::ws_handle& ws_handle,
                        const wsrep::ws_meta& ws_meta) override
  {
    DBUG_ENTER("Wsrep_prepared_applier_service::start_transaction");
    DBUG_RETURN(m_thd->wsrep_cs().start_transaction(ws_handle, ws_meta));
  }
  int apply_write_set(const wsrep::ws_meta& ws_meta,
                      const wsrep::const_buffer& data,
                      wsrep::mutable_buffer&) override
  {
    DBUG_ENTER("Wsrep_prepared_applier_service::apply_write_set");
    if (!wsrep::commits_transaction(ws_meta.flags()))
    {
      m_thd->wsrep_cs().fragment_applied(ws_meta.seqno());
    }
    if (wsrep::prepares_transaction(ws_meta.flags()))
    {
      wsrep_trans_xa_attach(m_thd, &m_xid);
    }
    DBUG_RETURN(0);
  }
  int commit(const wsrep::ws_handle& ws_handle,
             const wsrep::ws_meta& ws_meta) override
  {
    DBUG_ENTER("Wsrep_prepared_applier_service::commit");
    DBUG_ASSERT(m_thd->wsrep_trx().state() == wsrep::transaction::s_prepared);
    m_thd->transaction->xid_state.xid_cache_element= 0;
    DBUG_RETURN(Wsrep_applier_service::commit(ws_handle, ws_meta));
  }
  int rollback(const wsrep::ws_handle& ws_handle,
               const wsrep::ws_meta& ws_meta) override
  {
    DBUG_ENTER("Wsrep_prepared_applier_service::rollback");
    DBUG_ASSERT(m_thd->wsrep_trx().state() == wsrep::transaction::s_prepared);
    m_thd->transaction->xid_state.xid_cache_element= 0;
    DBUG_RETURN(Wsrep_applier_service::rollback(ws_handle, ws_meta));
  }
  int adopt_transaction(const wsrep::transaction& transaction) override
  {
    DBUG_ENTER("Wsrep_prepared_applier_service::adopt_transaction");
    m_thd->wsrep_cs().adopt_transaction(transaction);
    int ret= wsrep_trans_xa_attach(m_thd, &m_xid);
    DBUG_RETURN(ret);
  }
protected:
  XID m_xid;
};

class Wsrep_prepared_replayer_service :
  public Wsrep_prepared_applier_service
{
public:
  Wsrep_prepared_replayer_service(THD* thd, THD* orig_thd, XID* xid)
    : Wsrep_prepared_applier_service(thd, orig_thd, xid)
    , m_orig_thd(orig_thd) { }
  int commit(const wsrep::ws_handle& ws_handle,
             const wsrep::ws_meta& ws_meta) override
  {
    DBUG_ENTER("Wsrep_prepared_applier_service::commit");
    DBUG_ASSERT(m_thd->wsrep_trx().state() == wsrep::transaction::s_prepared);
    DBUG_RETURN(Wsrep_applier_service::commit(ws_handle, ws_meta));
  }
  int rollback(const wsrep::ws_handle& ws_handle,
               const wsrep::ws_meta& ws_meta) override
  {
    DBUG_ENTER("Wsrep_prepared_applier_service::rollback");
    DBUG_ASSERT(m_thd->wsrep_trx().state() == wsrep::transaction::s_prepared);
    DBUG_RETURN(Wsrep_applier_service::rollback(ws_handle, ws_meta));
  }
  int adopt_transaction(const wsrep::transaction& transaction) override
  {
    m_thd->wsrep_cs().adopt_transaction(transaction);
    m_thd->wsrep_cs().clone_transaction_for_replay(m_orig_thd->wsrep_trx());
    return false;
  }
  int replay();
private:
  THD* m_orig_thd;
  int replay_empty();
  int replay_from_storage();
};

class Wsrep_replayer_service : public Wsrep_high_priority_service
{
public:
  Wsrep_replayer_service(THD* replayer_thd, THD* orig_thd);
  ~Wsrep_replayer_service() override;
  int apply_write_set(const wsrep::ws_meta&, const wsrep::const_buffer&,
                      wsrep::mutable_buffer&) override;
  int apply_nbo_begin(const wsrep::ws_meta&, const wsrep::const_buffer& data,
                      wsrep::mutable_buffer& err) override
  {
    DBUG_ASSERT(0); /* DDL should never cause replaying */
    return 0;
  }
  void after_apply() override { }
  bool is_replaying() const override { return true; }
  void replay_status(enum wsrep::provider::status status)
  { m_replay_status = status; }
  enum wsrep::provider::status replay_status() const
  { return m_replay_status; }
  /* Replayer should never be forced to exit */
  bool check_exit_status() const override { return false; }
private:
  THD* m_orig_thd;
  struct da_shadow
  {
    enum Diagnostics_area::enum_diagnostics_status status;
    ulonglong affected_rows;
    ulonglong last_insert_id;
    char message[MYSQL_ERRMSG_SIZE];
    da_shadow()
      : status()
      , affected_rows()
      , last_insert_id()
      , message()
    { }
  } m_da_shadow;
  enum wsrep::provider::status m_replay_status;
};

#endif /* WSREP_HIGH_PRIORITY_SERVICE_H */
