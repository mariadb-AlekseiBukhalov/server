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
#include "mysql/psi/mysql_socket.h"

#include <cerrno>
#include <cstring>
#include <memory>

PSI_socket_key key_socket_wsrep;
static std::unique_ptr<wsrep::tls_service> entrypoint;

enum Wsrep_error_category
{
  Wsrep_tls_system_error,
  Wsrep_tls_ssl_error
};

static enum Wsrep_error_category system_error_category= Wsrep_tls_system_error;
static enum Wsrep_error_category ssl_error_category= Wsrep_tls_ssl_error;

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
    return (sys_error == 0 ? eof : error);
  case SSL_ERROR_SSL:
    {
      last_error= sys_error;
      last_error_category= &ssl_error_category;
      last_verify_error= SSL_get_verify_result(ssl);
      return wsrep::tls_service::error;
    }
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
      WSREP_WARN("SSL init error: %d", error);
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
      WSREP_WARN("SSL init error: %d", error);
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
  struct sockaddr_storage addr{};
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

wsrep::tls_service* wsrep_tls_service_init()
{
  entrypoint = std::unique_ptr<wsrep::tls_service>(new Wsrep_tls_service);
  return entrypoint.get();
}

void wsrep_tls_service_deinit()
{
  entrypoint.reset();
}
