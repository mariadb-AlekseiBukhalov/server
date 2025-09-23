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

#ifndef PLUGIN_GALERA_REPLICATION_H
#define PLUGIN_GALERA_REPLICATION_H

#include <mysql/plugin.h>
#include <my_compiler.h>

#ifdef __cplusplus
extern "C"
{
#endif

  /*************************************************************************
    API for Galera replication plugin (MYSQL_REPLICATION_PLUGIN).
  */

#define MYSQL_GALERA_REPLICATION_INTERFACE_VERSION 0x0200

/*
  Helper macros and type definitions.
 */
#define GRP_WARN_UNUSED_RESULT __attribute__((warn_unused_result))

  /** PSI socket key type. This must match the type used in
    mysql/psi/psi.h */
  typedef unsigned int PSI_socket_key;

  /** Client session identifier. */
  typedef unsigned long long grp_client_id_t;

  /** Transaction identifier. */
  typedef unsigned long long grp_trx_id_t;

  /** Total order sequence number. */
  typedef long long grp_seqno_t;

  /** Identifier data type. */
  typedef struct st_grp_id
  {
    /** Binary identifier data buffer. */
    char data[16];
    /** Actual size of the identifier data in bytes. */
    size_t size;
  } MY_ALIGNED(8) grp_id_t;

  typedef enum
  {
    /** In replication group, using primary view to keep it consistent
        with original terminology. */
    GRP_VIEW_PRIMARY,
    /** Replication group uncreachable (e.g. non-primary). */
    GRP_VIEW_NON_PRIMARY,
    /** Disconnected from replication group. */
    GRP_VIEW_DISCONNECTED
  } grp_view_type_t;

/** Maximum length of logical member name in grp_member_t.
    The length is sufficient to accommodate typical hostnames. */
#define GRP_MEMBER_NAME_LEN 256
/** Maximum length of incoming address in grp_member_t. */
#define GRP_INCOMING_ADDRESS_LEN 256
  typedef struct st_grp_member
  {
    /** Unique identifier of the member. */
    grp_id_t id;
    /** Optional unique human readable name. */
    char name[GRP_MEMBER_NAME_LEN];
    /** Optional incoming address. */
    char incoming_address[GRP_INCOMING_ADDRESS_LEN];
  } grp_member_t;

  /** Container for members. */
  typedef struct st_grp_members
  {
    const grp_member_t *members;
    size_t size;
  } grp_members_t;

  /** Structure representing cluster configuration view event */
  typedef struct st_grp_view
  {
    /** View type */
    grp_view_type_t type;
    /** View identifier, representing cluster identifier */
    grp_id_t view_id;
    /** Sequence number corresponding to view event */
    grp_seqno_t seqno;
    /** Members in the view */
    grp_members_t members;
    /** The index of the entry in the members list representing
     *  this node. If the node is not part of the view, set to -1. */
    ssize_t own_index;
    /** Internal position of the plugin, exposed so that it can
     *  be persisted for crash recovery. */
    int64_t provider_position;
  } grp_view_t;

  /** Helper struct for passing immutable data over the API */
  typedef struct st_grp_const_buf
  {
    /** Pointer to data buffer */
    const void *data;
    /** Size of the memory region pointed by the data pointer */
    size_t size;
  } grp_const_buf_t;

  /*
    Interface definitions.
   */

  /** Certification result. */
  typedef enum
  {
    /** Certification was success, total order sequence number assigned. */
    GRP_CERT_SUCCESS,
    /** Certification failed because of key conflict. */
    GRP_CERT_CONFLICT,
    /** Write set was rejected for other reason. */
    GRP_CERT_REJECT,
    /** The request will be logged without certification. */
    GRP_CERT_SKIP,
    /** The request is canceled (possibly due to the shutdown). */
    GRP_CERT_CANCEL,
    /** The request was not certified. */
    GRP_CERT_NONE,
  } grp_cert_result_t;

  /** Certification meta information. */
  typedef struct st_grp_cert_meta
  {
    /** The cluster id */
    grp_id_t cluster_id;
    /** Originating member ID. */
    grp_id_t id;
    /** Sequence number assigned in case of successful certification. */
    grp_seqno_t seqno;
    /** Sequence number this write set depends on. */
    int64_t depends_on;
    /** Replication position of the provider (log index). */
    int64_t provider_position;
  } grp_cert_meta_t;

  /** Key set node representation. */
  typedef struct st_grp_key_set_node
  {
    /** Path of the key. */
    grp_const_buf_t path;
    /** Type of the key node. */
    int16_t type;
    /** Array of child key nodes ordered by their paths. */
    struct st_grp_key_set_node *nodes;
    /** Number of elements in the nodes array. */
    int64_t nodes_size;
  } grp_key_set_node_t;

  /** Write set kind enumeration. */
  typedef enum
  {
    /** Total order operation. */
    GRP_KIND_TOI,
    /** NBO operation begin. */
    GRP_KIND_NBO_BEGIN,
    /** NBO operation end. */
    GRP_KIND_NBO_END,
    /** Single write set. */
    GRP_KIND_TRX_ATOMIC,
    /** SR-transaction initial fragment. */
    GRP_KIND_TRX_START,
    /** SR-transaction follow-up fragment. */
    GRP_KIND_TRX_FRAGMENT,
    /** SR-transaction prepare. */
    GRP_KIND_TRX_PREPARE,
    /** SR-transaction commit. */
    GRP_KIND_TRX_COMMIT,
    /** SR-transaction rollback. */
    GRP_KIND_TRX_ROLLBACK,
    /** Not assigned. */
    GRP_KIND_NONE= 255,
  } grp_ws_kind_t;

  /** Write set parallel applying policy. */
  typedef enum st_grp_pa_policy
  {
    /** Write set is safe to apply in parallel. */
    GRP_PA_SAFE,
    /** Write set is safe to apply in parallel, but must not start
     *  applying before transaction with seqno last_committed has
     *  committed. */
    GRP_PA_LAST_COMMITTED,
    /** Parallel applying is unsafe. */
    GRP_PA_UNSAFE
  } grp_ws_pa_policy_t;

  /** Write set descriptor. */
  typedef struct st_grp_write_set
  {
    /********** Input parameters **********/
    /** Client session identifier */
    grp_client_id_t client_id;
    /** Transaction identifier */
    grp_trx_id_t transaction_id;
    /** Read view. */
    grp_seqno_t read_view;
    /** Last committed at the beginning of certification. */
    grp_seqno_t last_committed;
    /** Write set kind. */
    grp_ws_kind_t kind;
    /** Parallel applying policy. */
    grp_ws_pa_policy_t pa_policy;
    /** Key set root. */
    grp_key_set_node_t keys;
    /** Buffer containing write set data. */
    grp_const_buf_t data;

    /********** Output parameters *********/
    /** Certification result assigned by GRP. */
    grp_cert_result_t cert_result;
    /** Meta information valid only upon successful certification. */
    grp_cert_meta_t cert_meta;
  } grp_write_set_t;

  /* Error voting result. */
  typedef enum
  {
    /** Apply error is consistent with other members. */
    GRP_VOTE_PASSED,
    /** Apply error is inconsistent with other memebers. */
    GRP_VOTE_REJECTED,
    /** The request is canceled (possibly due to the shutdown). */
    GRP_VOTE_CANCELED,
    /** The voting has not completed. */
    GRP_VOTE_NONE,
  } grp_vote_result_t;

  typedef struct st_grp_apply_error
  {
    /********** Input parameters **********/
    /** Byte array that contains error description. Empty for no error */
    grp_const_buf_t data;
    /** Client identifier */
    grp_id_t sender_id;
    /** Seqno assigned to the event that failed to apply. */
    grp_seqno_t seqno;
    /** Log position of the event that failed to apply. */
    int64_t position;
  } grp_apply_error_t;

  /* Type tag for server side command handler context. */
  typedef struct st_grp_command_handler_context grp_command_handler_context_t;
  /** Command handler. Passed to various calls which require custom handling.
   */
  typedef struct st_grp_command_handler
  {
    /** Handle the command on plugin side.
     *
     * @param ctx Context which was provided by the caller.
     * @param error_code Error code for operation, zero means success.
     * @param ptr Command specific data set by the plugin.
     */
    void (*handle)(grp_command_handler_context_t *ctx, int error_code,
                   const void *ptr);
    /** Context pointer passed to the handle function. */
    grp_command_handler_context_t *ctx;
  } grp_command_handler_t;

  /** Log message levels. */
  typedef enum enum_grp_log_level
  {
    GRP_LOG_ERROR,
    GRP_LOG_WARN,
    GRP_LOG_INFO,
    GRP_LOG_DEBUG,
    GRP_LOG_LEVEL_MAX= GRP_LOG_DEBUG
  } grp_log_level_t;

  /** Enumeration for node isolation mode. */
  typedef enum enum_grp_node_isolation
  {
    /** Can communicate with cluster */
    GRP_NODE_ISOLATION_NONE,
    /** Keep service running but close connections. */
    GRP_NODE_ISOLATION_CONNECTIVITY,
    /** Force disconnect without graceful leave. */
    GRP_NODE_ISOLATION_DISCONNECTED
  } grp_node_isolation_t;

  /* Enumeration for allowlist key type. */
  typedef enum enum_grp_allowlist_key
  {
    /* IP check. */
    GRP_ALLOWLIST_IP,
    /* SSL check. */
    GRP_ALLOWLIST_SSL
  } grp_allowlist_key_t;

  /** Type tag for server side handler context. */
  typedef struct st_grp_handler_context grp_handler_context_t;

  /** Callbacks for handling galera replication events. */
  typedef struct st_grp_handler
  {
    /** Called when successfully connected to the cluster.
     * @param ctx Handler context
     * @param view Initial view after connection
     * @return 0 on success, non-zero on error
     */
    int (*handle_connected)(grp_handler_context_t *,
                            const grp_view_t *) GRP_WARN_UNUSED_RESULT;
    /** Called when cluster view changes.
     * @param ctx Handler context
     * @param view New cluster view
     * @return 0 on success, non-zero on error
     */
    int (*handle_view)(grp_handler_context_t *,
                       const grp_view_t *) GRP_WARN_UNUSED_RESULT;
    /** Called when a write set needs to be applied.
     * @param ctx Handler context
     * @param write_set Write set to apply
     * @param local True if write set originated from this node
     * @return 0 on success, non-zero on error
     */
    int (*handle_write_set)(grp_handler_context_t *, grp_write_set_t *,
                            bool) GRP_WARN_UNUSED_RESULT;
    /** Called when an error vote needs to be handled.
     * @return 0 on success, non-zero on error
     */
    int (*handle_error_vote)(grp_handler_context_t *, grp_apply_error_t *,
                             grp_vote_result_t, bool) GRP_WARN_UNUSED_RESULT;
    /** Called to synchronize with the cluster.
     * @param ctx Handler context
     * @return 0 on success, non-zero on error
     */
    int (*handle_sync)(grp_handler_context_t *) GRP_WARN_UNUSED_RESULT;
    /** Called when SST (State Snapshot Transfer) is requested.
     * @param ctx Handler context
     * @param method SST method to use
     * @param receive_address Address where SST should be sent
     * @return 0 on success, non-zero on error
     */
    int (*handle_sst_request)(grp_handler_context_t *, const char *method,
                              const char *receive_address)
        GRP_WARN_UNUSED_RESULT;
    /** Get the last committed sequence number.
     * @param ctx Handler context
     * @return Last committed sequence number
     */
    grp_seqno_t (*get_last_committed)(const grp_handler_context_t *);
    /** Called on a new connection attempt to check if the connection
     * is allowed.
     * @return True if connection is allowed, false otherwise
     **/
    bool (*allowlist_check)(grp_handler_context_t *, grp_allowlist_key_t,
                            grp_const_buf_t) GRP_WARN_UNUSED_RESULT;
    /** Log a message with specified level.
     * @param level Log level
     * @param msg Message to log
     */
    void (*log_message)(grp_log_level_t, const char *msg);

    /**
     * Get PSI_socket_key for plugin connections
     */
    PSI_socket_key (*get_psi_socket_key)();
    /** Server side context passed to callback functions. */
    grp_handler_context_t *context;
  } grp_handler_t;

  /** Cluster-wide configuration options.
   * Configuration options which are set on bootstrap node
   * and need to be consistently set in the cluster will be set
   * here. */
  typedef struct st_grp_cluster_config
  {
    /** Wsrep GTID Domain ID. The domain ID from bootstrap
     * node will be the effective one. */
    uint wsrep_gtid_domain_id;
  } grp_cluster_config_t;

  typedef struct st_grp_connect_args
  {
    /** Name of the cluster. */
    const char *cluster_name;
    /** Human readable name of the node. */
    const char *node_name;
    /** Cluster address. */
    const char *cluster_address;
    /** Node incoming address. */
    const char *incoming_address;
    /** Cluster config  */
    grp_cluster_config_t cluster_config;
    /** Start position. */
    int64_t start_position;
    /** Bootstrap option. */
    bool bootstrap;
  } grp_connect_args_t;
  /*
    Galera replication plugin descriptor.

    Implements a custom provider functionality for WSREP.
  */
  struct st_mysql_galera_replication
  {
    /** Plugin interface version number. */
    int interface_version;

    /** Initialize replication plugin with node address and handlers.
     * @param node_address Address of this node
     * @param handler Event handlers for replication events
     * @return 0 on success, non-zero on error
     */
    int (*init)(const char *node_address, grp_handler_t *handler);
    /** Deinitialize replication plugin and cleanup resources. */
    void (*deinit)();
    /** Run replication service event loop.
     * @return 0 on success, non-zero on error
     */
    int (*run)();
    /** Connect to cluster with specified parameters.
     * @param args Connection arguments including cluster address, node name, etc.
     * @return 0 on success, non-zero on error
     */
    int (*connect)(const grp_connect_args_t *args);
    /** Disconnect from cluster gracefully.
     * @return 0 on success, non-zero on error
     */
    int (*disconnect)();
    /** Perform a synchronous operation in the cluster. The command handler
        will be called when it can be guaranteed that all causally preceding
        operations have been applied by the plugin.
     * @param handler Command handler to call when sync is complete
     * @return 0 on success, non-zero on error
     */
    int (*sync_wait)(grp_command_handler_t handler);
    /** Report error on applying the action. */
    int (*error_on_apply)(grp_apply_error_t*);
    /** Certify a write set. The certification result will be delivered
        via handle_write_set() callback. The caller must ensure that the
        @p write_set remains valid until the handle_write_set() has been
        called. Return zero on success, non-zero system error code on
        failure. In case of error, the write_set can be freed.
     * @param write_set Write set to certify
     * @return 0 on success, non-zero on error
     */
    int (*certify)(grp_write_set_t *write_set);
    /** Release event on plugin side after it has been fully committed.
     * @param position Provider position to release
     */
    void (*release_committed)(int64_t position);
    /** Desync node from the cluster flow control.
     * @param handler Command handler to call when desync is complete
     * @return 0 on success, non-zero on error
     */
    int (*desync)(grp_command_handler_t handler);
    /** Resync node back to cluster flow control.
     * @param handler Command handler to call when resync is complete
     * @return 0 on success, non-zero on error
     */
    int (*resync)(grp_command_handler_t handler);
    /** Send a request to pause the provider.
     * @param handler Command handler to call when pause is complete
     * @return 0 on success, non-zero on error
     */
    int (*pause)(grp_command_handler_t handler);
    /** Send a request to resume the provider.
     * @param handler Command handler to call when resume is complete
     * @return 0 on success, non-zero on error
     */
    int (*resume)(grp_command_handler_t handler);
    /** Report SST donation completion.
     * @param seqno Sequence number of the donated state
     * @param err Error code, 0 for success
     * @return 0 on success, non-zero on error
     */
    int (*sst_sent)(grp_seqno_t seqno, int err);
    /** Set isolation mode for the node.
     * @param isolation Isolation mode to set
     * @return 0 on success, non-zero on error
     */
    int (*set_node_isolation)(grp_node_isolation_t isolation);
  };

#ifdef __cplusplus
}
#endif

#endif /* PLUGIN_GALERA_REPLICATION_H */
