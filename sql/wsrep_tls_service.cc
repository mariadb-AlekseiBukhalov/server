/* Copyright 2020-2021 Codership Oy <info@codership.com>

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

/*
  TLS hooks for wsrep provider.

  If enabled, wsrep provider will delegate all TLS/SSL related operations
  to MariaDB side hooks and use CA, certificate and key configured for
  MariaDB. The connections are managed on provider side.
*/

#include "wsrep_tls_service.h"
#include "wsrep_thread_service.h"

#include "my_global.h"
#include "wsrep_mysqld.h"
#include "violite.h"

#ifdef HAVE_OPENSSL
#include "ssl_compat.h"
#endif /* HAVE_OPENSSL */

#include "mysql/psi/mysql_socket.h"
#include "mysql/psi/mysql_thread.h"

#include <cerrno>
#include <cstring>
#include <memory>

PSI_socket_key key_socket_wsrep;
PSI_thread_key key_wsrep_cert_alert_thread;
static pthread_t wsrep_cert_alert_thread;
PSI_mutex_key key_LOCK_wsrep_cert_alert;
static mysql_mutex_t wsrep_cert_alert_mutex;
PSI_cond_key key_COND_wsrep_cert_alert;
static mysql_cond_t wsrep_cert_alert_cond;
enum enum_wsrep_cert_alert_thread_state
{
  wsrep_cert_alert_thread_idle,
  wsrep_cert_alert_thread_running,
  wsrep_cert_alert_thread_aborted
} wsrep_cert_alert_thread_state;

static std::unique_ptr<wsrep::tls_service> entrypoint;

enum Wsrep_error_category
{
  Wsrep_tls_system_error,
  Wsrep_tls_ssl_error
};

static enum Wsrep_error_category system_error_category= Wsrep_tls_system_error;
static enum Wsrep_error_category ssl_error_category= Wsrep_tls_ssl_error;

static bool wsrep_cert_expires(const ASN1_TIME *not_after,
                               time_t hours_until_warning)
{
  time_t expiration_time= ::time(nullptr) + 3600*hours_until_warning;
  bool ret= false;
  if (X509_cmp_time(not_after, &expiration_time) < 0)
  {
    ret= true;
  }
  return ret;
}

const char* wsrep_asn1_print(const ASN1_TIME *asn1, char *buf, size_t len)
{
  BIO *bio= BIO_new(BIO_s_mem());
  if (bio)
  {
    if (ASN1_TIME_print(bio, asn1))
    {
      BIO_read(bio, buf, (int)(len - 1));
    }
    BIO_free(bio);
  }
  return buf;
}

static void wsrep_cert_report_expiration(SSL *ssl, const ASN1_TIME *not_after)
{
  MYSQL_SOCKET socket{ SSL_get_fd(ssl), 0, 0, 0, NULL };
  struct sockaddr_storage addr;
  ::memset(&addr, 0, sizeof(addr));
  socklen_t addr_len(sizeof(addr));
  uint port= 0;
  char addr_str[INET6_ADDRSTRLEN] = {0, };
  if (mysql_socket_getsockname(socket, (struct sockaddr*)&addr, &addr_len) ||
      (addr.ss_family != AF_INET && addr.ss_family != AF_INET6))
  {
    snprintf(addr_str, sizeof(addr_str), "unknown");
  }
  else if (addr.ss_family == AF_INET)
  {
    inet_ntop(AF_INET, &((struct sockaddr_in*)&addr)->sin_addr,
              addr_str, sizeof(addr_str));
    port= ntohs(((struct sockaddr_in*)&addr)->sin_port);
  }
  else if (addr.ss_family == AF_INET6)
  {
    inet_ntop(AF_INET6, &((struct sockaddr_in6*)&addr)->sin6_addr,
              addr_str, sizeof(addr_str));
    port= ntohs(((struct sockaddr_in6*)&addr)->sin6_port);
  }
  char time_str_buf[32] = {0};
  WSREP_WARN("certificate expiration warning: the peer certificate for "
             "%s:%u expires at %s",
             addr_str, port,
             wsrep_asn1_print(not_after, time_str_buf, sizeof(time_str_buf)));
}

static void wsrep_warn_peer_certificate_expiration(SSL *ssl)
{
  const X509 *cert= SSL_get_peer_certificate(ssl);
  if (cert)
  {
    const ASN1_TIME *not_after= X509_get0_notAfter(cert);

    mysql_mutex_lock(&LOCK_global_system_variables);
    ulonglong hours_until_warning= wsrep_certificate_expiration_hours_warning;
    mysql_mutex_unlock(&LOCK_global_system_variables);
    if (wsrep_cert_expires(not_after, time_t(hours_until_warning)))
    {
      wsrep_cert_report_expiration(ssl, not_after);
    }
  }
}

class Wsrep_tls_service : public wsrep::tls_service
{
  struct Stream : public wsrep::tls_stream
  {
    MYSQL_SOCKET mysql_socket;
    struct st_VioSSLFd* vio_ssl;
    SSL* ssl;
    int last_error;
    int last_verify_error;
    const void* last_error_category;
    bool handshake_complete;
    Stream(int fd)
      : mysql_socket()
      , vio_ssl() /* Lazy initialized in handshake */
      , ssl() /* Lazy initialized in handshake */
      , last_error()
      , last_verify_error()
      , last_error_category()
      , handshake_complete()
    {
      mysql_socket.fd = fd;
    }
    ~Stream()
    {
      if (ssl) SSL_free(ssl);
      if (vio_ssl)
      {
        SSL_CTX_free(vio_ssl->ssl_context);
        my_free(vio_ssl);
      }
    }
    /* Initialize SSL structures. Refer to vio/viossl.c:ssl_do() */
    int alloc_and_init_ssl();

    /* Map SSL return codes to tls_service status and set error info. */
    enum wsrep::tls_service::status
    map_status(int ssl_error, unsigned long sys_error, const char* op);

    wsrep::tls_service::status client_handshake();
    wsrep::tls_service::status server_handshake();

    /* Read from SSL stream */
    wsrep::tls_service::op_result read(void* buf, size_t max_count);

    /* Write to SSL stream */
    wsrep::tls_service::op_result write(const void* buf, size_t count);

    wsrep::tls_service::status shutdown();

    /* Clear current error status */
    void clear_error()
    {
      last_error = 0;
      last_verify_error = 0;
      last_error_category = 0;
    }


    mutable std::string last_error_message_str;
    const char* last_error_message() const
    {
      if (last_error_category == &system_error_category)
        return ::strerror(last_error);
      if (last_error_category == &ssl_error_category)
      {
        last_error_message_str.clear();
        last_error_message_str += ERR_reason_error_string(last_error);
        if (last_verify_error)
        {
          last_error_message_str += ": ";
          last_error_message_str +=
            X509_verify_cert_error_string(last_verify_error);
        }
        return last_error_message_str.c_str();
      }
      return "unknown";
    }
  };
public:
  wsrep::tls_stream* create_tls_stream(int fd) WSREP_NOEXCEPT override;

  void destroy(wsrep::tls_stream* stream) WSREP_NOEXCEPT override;

  wsrep::tls_service::status client_handshake(wsrep::tls_stream* stream)
    WSREP_NOEXCEPT override;

  wsrep::tls_service::status server_handshake(wsrep::tls_stream*)
    WSREP_NOEXCEPT override;

  wsrep::tls_service::op_result read(
    wsrep::tls_stream* stream, void* buf, size_t max_count)
    WSREP_NOEXCEPT override;

  wsrep::tls_service::op_result write(
    wsrep::tls_stream* stream, const void* buf, size_t count)
    WSREP_NOEXCEPT override;

  wsrep::tls_service::status shutdown(wsrep::tls_stream*)
    WSREP_NOEXCEPT override;

  int get_error_number(const wsrep::tls_stream* stream)
    const WSREP_NOEXCEPT override
  {
    const Stream* st= static_cast<const Stream*>(stream);
    return st->last_error;
  }

  const void* get_error_category(const wsrep::tls_stream* stream)
    const WSREP_NOEXCEPT override
  {
    const Stream* st= static_cast<const Stream*>(stream);
    return st->last_error_category;
  }

  const char* get_error_message(const wsrep::tls_stream* stream,
                                int value, const void*)
    const WSREP_NOEXCEPT override
  {
    const Stream* st= static_cast<const Stream*>(stream);
    return st->last_error_message();
  }
private:
#ifdef HAVE_PSI_SOCKET_INTERFACE
  void register_socket(MYSQL_SOCKET* socket);
  void deregister_socket(MYSQL_SOCKET* socket);
#endif /* HAVE_PSI_SOCKET_INTERFACE */
};


#ifdef HAVE_WOLFSSL
static int wsrep_wolfssl_return(int ret, int on_block)
{
  if (ret < 0)
  {
    int err = errno;
    if (err == EWOULDBLOCK || err == EAGAIN)
    {
      ret= on_block;
    }
    else if (err == EINTR)
    {
      ret= WOLFSSL_CBIO_ERR_ISR;
    }
    else if (err == EPIPE || err == ECONNABORTED || err == ECONNRESET)
    {
      ret= WOLFSSL_CBIO_ERR_CONN_CLOSE;
    }
    else
    {
      ret= WOLFSSL_CBIO_ERR_GENERAL;
    }
  }
  else if (ret == 0)
  {
    ret= WOLFSSL_CBIO_ERR_CONN_CLOSE;
  }
  return ret;
}

static int wsrep_wolfssl_recv(WOLFSSL* ssl, char* buf, int sz, void*)
{
  return wsrep_wolfssl_return(recv(SSL_get_fd(ssl), buf, sz, 0),
                              WOLFSSL_CBIO_ERR_WANT_READ);
}

static int wsrep_wolfssl_send(WOLFSSL* ssl, char* buf, int sz, void*)
{
  return wsrep_wolfssl_return(send(SSL_get_fd(ssl), buf, sz, MSG_NOSIGNAL),
                              WOLFSSL_CBIO_ERR_WANT_WRITE);
}
#endif /* HAVE_WOLFSSL */

/*
  Stream
*/
int Wsrep_tls_service::Stream::alloc_and_init_ssl()
{
  DBUG_ASSERT(vio_ssl->ssl_context);

  switch (wsrep_ssl_mode)
  {
  case WSREP_SSL_MODE_SERVER:
    SSL_CTX_set_verify(vio_ssl->ssl_context, SSL_VERIFY_NONE, NULL);
    break;
  case WSREP_SSL_MODE_SERVER_X509:
    SSL_CTX_set_verify(vio_ssl->ssl_context, SSL_VERIFY_PEER, NULL);
    break;
  default:
    WSREP_WARN("Unknown wsrep_ssl_mode, not changing SSL verify mode");
    break;
  }
  ssl= SSL_new(vio_ssl->ssl_context);
  if (!ssl)
  {
    last_error= ENOMEM;
    last_error_category= &system_error_category;
    return 1;
  }
  my_socket sd= mysql_socket_getfd(mysql_socket);
  SSL_set_fd(ssl, (int)sd);
#if defined(SSL_OP_NO_COMPRESSION)
  SSL_set_options(ssl, SSL_OP_NO_COMPRESSION);
#endif
#ifdef HAVE_WOLFSSL
  wolfSSL_SSLSetIORecv(ssl, wsrep_wolfssl_recv);
  wolfSSL_SSLSetIOSend(ssl, wsrep_wolfssl_send);
#endif /* HAVE_WOLFSSL */

  return 0;
}

enum wsrep::tls_service::status
Wsrep_tls_service::Stream::map_status(
  int ssl_error, unsigned long sys_error, const char* op)
{
  switch (ssl_error)
  {
  case SSL_ERROR_NONE:
    return wsrep::tls_service::success;
  case SSL_ERROR_WANT_WRITE:
    return wsrep::tls_service::want_write;
  case SSL_ERROR_WANT_READ:
    return wsrep::tls_service::want_read;
  case SSL_ERROR_SYSCALL:
    last_error= sys_error;
    last_error_category= &system_error_category;
    return (sys_error == 0 ? wsrep::tls_service::eof
                           : wsrep::tls_service::error);
  case SSL_ERROR_SSL:
    {
      last_error= sys_error;
      last_error_category= &ssl_error_category;
      last_verify_error= SSL_get_verify_result(ssl);
      return wsrep::tls_service::error;
    }
  case SSL_ERROR_ZERO_RETURN:
    return eof;
  default:
    /* WolfSSL returns negative ssl_errors on socket errors. We treat
     * them as EOF to avoid excessive warning messages. All unhandled
     * positive SSL errors are reported as errors. */
    last_error= ssl_error;
    last_error_category= &ssl_error_category;
    last_verify_error= SSL_get_verify_result(ssl);
    return (ssl_error < 0 ? wsrep::tls_service::eof : wsrep::tls_service::error);
  }
  DBUG_ASSERT(0);
  return wsrep::tls_service::error;
}

wsrep::tls_service::status
Wsrep_tls_service::Stream::client_handshake()
{
  clear_error();
  auto result= SSL_connect(ssl);
  auto ssl_error= SSL_get_error(ssl, result);
  auto sys_error= ERR_get_error();
  auto ret= map_status(ssl_error, sys_error, "client handshake");
  if (ret == wsrep::tls_service::success)
  {
    wsrep_warn_peer_certificate_expiration(ssl);
    handshake_complete= true;
  }
  return ret;
}

wsrep::tls_service::status
Wsrep_tls_service::Stream::server_handshake()
{
  clear_error();
  auto result= SSL_accept(ssl);
  auto ssl_error= SSL_get_error(ssl, result);
  auto sys_error= ERR_get_error();
  auto ret= map_status(ssl_error, sys_error, "server handshake");
  if (ret == wsrep::tls_service::success)
  {
    wsrep_warn_peer_certificate_expiration(ssl);
    handshake_complete= true;
  }
  return ret;
}

wsrep::tls_service::op_result
Wsrep_tls_service::Stream::read(void* buf, size_t max_count)
{
  clear_error();
  size_t bytes_transferred(0);
  auto result= SSL_read(ssl, buf, max_count);
  auto ssl_error= SSL_get_error(ssl, result);
  auto sys_error= ERR_get_error();
  if (ssl_error == SSL_ERROR_WANT_READ &&
      (bytes_transferred = SSL_pending(ssl)) > 0)
  {
    result = SSL_read(ssl, buf, bytes_transferred);
    DBUG_ASSERT(static_cast<size_t>(result) == bytes_transferred);
    return op_result{map_status(ssl_error, sys_error, "read"),
        bytes_transferred};
  }
  else if (result > 0)
  {
    bytes_transferred = result;
  }
  return op_result{map_status(ssl_error, sys_error, "read"),
      bytes_transferred};
}

wsrep::tls_service::op_result
Wsrep_tls_service::Stream::write(const void* buf, size_t count)
{
  clear_error();
  size_t bytes_transferred= 0;
  auto result= SSL_write(ssl, buf, count);
  auto ssl_error= SSL_get_error(ssl, result);
  auto sys_error= ERR_get_error();
  if (result > 0)
  {
    bytes_transferred = result;
  }
  return op_result{map_status(ssl_error, sys_error, "write"),
      bytes_transferred};
}

wsrep::tls_service::status
Wsrep_tls_service::Stream::shutdown()
{
  if (!ssl)
  {
    last_error= EINVAL;
    last_error_category= &system_error_category;
    return wsrep::tls_service::error;
  }

  /* To suppress 'shutdown while in init' warning. */
  if (!handshake_complete)
  {
    return wsrep::tls_service::success;
  }
  /* Provider messages are enveloped with a header containing length of the
     message, so they are not vulnerable for truncation attacks. Make
     SSL shutdown silent. */
  SSL_set_quiet_shutdown(ssl, 1);
  auto result= SSL_shutdown(ssl);
  wsrep::tls_service::status ret;
  switch (result)
  {
  case 0:
  case 1:
    ret= wsrep::tls_service::success;
    break;
  default:
    {
      auto ssl_error= SSL_get_error(ssl, result);
      auto sys_error= ERR_get_error();
      ret= map_status(ssl_error, sys_error, "shutdown");
    }
    break;
  }
  return ret;
}

/*
  Service
 */

wsrep::tls_stream*
Wsrep_tls_service::create_tls_stream(int fd) WSREP_NOEXCEPT
{
  Stream* ret(new(std::nothrow) Stream(fd));
#ifdef HAVE_PSI_SOCKET_INTERFACE
  if (ret && wsrep_thread_service_initialized())
  {
    register_socket(&ret->mysql_socket);
  }
#endif /* HAVE_PSI_SOCKET_INTERFACE */
  return ret;
}

void Wsrep_tls_service::destroy(wsrep::tls_stream* stream) WSREP_NOEXCEPT
{
    Stream* st(static_cast<Stream*>(stream));
#ifdef HAVE_PSI_SOCKET_INTERFACE
    if (wsrep_thread_service_initialized())
    {
      deregister_socket(&st->mysql_socket);
    }
#endif /* HAVE_PSI_SOCKET_INTERFACE */
    delete st;
}

wsrep::tls_service::status
Wsrep_tls_service::client_handshake(wsrep::tls_stream* stream) WSREP_NOEXCEPT
{
  Stream* st(static_cast<Stream*>(stream));
  if (!st->vio_ssl)
  {
    enum enum_ssl_init_error error= SSL_INITERR_NOERROR;
    st->vio_ssl= new_VioSSLConnectorFd(opt_ssl_key, opt_ssl_cert,
                                       opt_ssl_ca, opt_ssl_capath,
                                       opt_ssl_cipher, &error,
                                       opt_ssl_crl, opt_ssl_crlpath);

    if (error != SSL_INITERR_NOERROR)
    {
      WSREP_WARN("SSL init error: %s", sslGetErrString(error));
      st->last_error= EPROTO;
      st->last_error_category= &system_error_category;
      return wsrep::tls_service::error;
    }

    if (st->alloc_and_init_ssl())
    {
      /* Last error and category set by alloc_and_init_ssl() */
      return wsrep::tls_service::error;
    }
  }

  return st->client_handshake();
}

wsrep::tls_service::status
Wsrep_tls_service::server_handshake(wsrep::tls_stream* stream)
    WSREP_NOEXCEPT
{
  Stream* st(static_cast<Stream*>(stream));
  if (!st->vio_ssl)
  {
    enum enum_ssl_init_error error= SSL_INITERR_NOERROR;
    st->vio_ssl= new_VioSSLAcceptorFd(opt_ssl_key, opt_ssl_cert,
                                      opt_ssl_ca, opt_ssl_capath,
                                      opt_ssl_cipher, &error,
                                      opt_ssl_crl, opt_ssl_crlpath,
                                      tls_version);
    if (error != SSL_INITERR_NOERROR)
    {
      WSREP_WARN("SSL init error: %s", sslGetErrString(error));
      st->last_error= EPROTO;
      st->last_error_category= &system_error_category;
      return wsrep::tls_service::error;
    }

    if (st->alloc_and_init_ssl())
    {
      /* Last error and category set by alloc_and_init_ssl() */
      return wsrep::tls_service::error;
    }
  }

  return st->server_handshake();
}

wsrep::tls_service::op_result
Wsrep_tls_service::read(
  wsrep::tls_stream* stream, void* buf, size_t max_count)
  WSREP_NOEXCEPT
{
  Stream* st= static_cast<Stream*>(stream);
  return st->read(buf, max_count);
}

wsrep::tls_service::op_result Wsrep_tls_service::write(
  wsrep::tls_stream* stream, const void* buf, size_t count)
  WSREP_NOEXCEPT
{
  Stream* st= static_cast<Stream*>(stream);
  return st->write(buf, count);
}

wsrep::tls_service::status Wsrep_tls_service::shutdown(
  wsrep::tls_stream* stream) WSREP_NOEXCEPT
{
  Stream* st= static_cast<Stream*>(stream);
  return st->shutdown();
}

#ifdef HAVE_PSI_SOCKET_INTERFACE
void Wsrep_tls_service::register_socket(MYSQL_SOCKET* socket)
{
  struct sockaddr_storage addr;
  ::memset(&addr, 0, sizeof(addr));
  socklen_t addr_len(sizeof(addr));
  if (mysql_socket_getsockname(*socket, (struct sockaddr*)&addr, &addr_len))
  {
    WSREP_WARN("Failed to get sockname: %d", errno);
    socket->m_psi= NULL;
  }
  else
  {
    socket->m_psi= PSI_SOCKET_CALL(init_socket)
      (key_socket_wsrep, (const my_socket*)&socket->fd,
       (const struct sockaddr*)&addr, addr_len);
  }
}

void Wsrep_tls_service::deregister_socket(MYSQL_SOCKET* socket)
{
  if (socket->m_psi)
  {
    PSI_SOCKET_CALL(destroy_socket)(socket->m_psi);
  }
}
#endif /* HAVE_PSI_SOCKET_INTERFACE */

static X509 *wsrep_get_local_cert()
{
  FILE *fp= fopen(opt_ssl_cert, "r");
  if (!fp)
  {
    WSREP_WARN("Could not read certificate file: %s", opt_ssl_cert);
    return nullptr;
  }
  X509 *cert= PEM_read_X509(fp, 0, 0, 0);
  fclose(fp);
  return cert;
}

extern "C" void* wsrep_cert_alert_run(void*)
{
  mysql_mutex_lock(&wsrep_cert_alert_mutex);
  while (wsrep_cert_alert_thread_state !=
         wsrep_cert_alert_thread_aborted)
  {
    if (wsrep_cert_alert_thread_state == wsrep_cert_alert_thread_idle)
    {
      wsrep_cert_alert_thread_state= wsrep_cert_alert_thread_running;
      mysql_cond_signal(&wsrep_cert_alert_cond);
    }
    mysql_mutex_lock(&LOCK_global_system_variables);
    ulonglong hours_until_warning= wsrep_certificate_expiration_hours_warning;
    mysql_mutex_unlock(&LOCK_global_system_variables);
    X509 *cert= wsrep_get_local_cert();
    const ASN1_TIME *not_after= cert ? X509_get0_notAfter(cert) : nullptr;
    if (hours_until_warning &&
        not_after &&
        wsrep_cert_expires(X509_get0_notAfter(cert),
                           time_t(hours_until_warning)))
    {
      char time_str_buf[32]= {0};
      WSREP_WARN(
          "certificate expiration warning: the certificate expires at %s",
          wsrep_asn1_print(not_after, time_str_buf, sizeof(time_str_buf)));
    }
    if (cert)
    {
      X509_free(cert);
    }
    struct timeval now;
    struct timespec wait_until;
    gettimeofday(&now, nullptr);
    wait_until.tv_sec= now.tv_sec + 3600;
    wait_until.tv_nsec = now.tv_usec * 1000;
    mysql_cond_timedwait(&wsrep_cert_alert_cond,
                         &wsrep_cert_alert_mutex,
                         &wait_until);
  }
  mysql_mutex_unlock(&wsrep_cert_alert_mutex);
  return nullptr;
}

static int wsrep_cert_alert_start()
{
  mysql_mutex_init(key_LOCK_wsrep_cert_alert,
                   &wsrep_cert_alert_mutex, 0);
  mysql_cond_init(key_COND_wsrep_cert_alert,
                  &wsrep_cert_alert_cond, 0);
  wsrep_cert_alert_thread_state = wsrep_cert_alert_thread_idle;
  if (mysql_thread_create(key_wsrep_cert_alert_thread,
                          &wsrep_cert_alert_thread, 0,
                          &wsrep_cert_alert_run, 0))
  {
    return 1;
  }
  mysql_mutex_lock(&wsrep_cert_alert_mutex);
  while (wsrep_cert_alert_thread_state == wsrep_cert_alert_thread_idle)
  {
    mysql_cond_wait(&wsrep_cert_alert_cond, &wsrep_cert_alert_mutex);
  }
  mysql_mutex_unlock(&wsrep_cert_alert_mutex);
  return 0;
}

static void wsrep_cert_alert_abort()
{
  mysql_mutex_lock(&wsrep_cert_alert_mutex);
  wsrep_cert_alert_thread_state= wsrep_cert_alert_thread_aborted;
  mysql_cond_signal(&wsrep_cert_alert_cond);
  mysql_mutex_unlock(&wsrep_cert_alert_mutex);
  pthread_join(wsrep_cert_alert_thread, 0);
}

wsrep::tls_service* wsrep_tls_service_init()
{
  if (wsrep_cert_alert_start())
  {
    WSREP_ERROR("Could not start TLS certificate alert thread");
    return nullptr;
  }
  entrypoint = std::unique_ptr<wsrep::tls_service>(new Wsrep_tls_service);
  return entrypoint.get();
}

void wsrep_tls_service_deinit()
{
  entrypoint.reset();
  wsrep_cert_alert_abort();
  mysql_mutex_destroy(&wsrep_cert_alert_mutex);
  mysql_cond_destroy(&wsrep_cert_alert_cond);
}
