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

#include "wsrep_thread_service.h"

#include "my_global.h"
#include "mysql/psi/psi.h"

#ifdef HAVE_PSI_THREAD_INTERFACE
#include "wsrep_mysqld.h"

#include <cstring>

#include <algorithm>
#include <memory>
#include <vector>

/*
  Thread, mutex and condvar instrumentation for wsrep provider.

  If enabled, the provider will use provided hooks to use
  MariaDB implementation of thread, mutex and condvar.

  The provider will provide memory area for mutexes and
  condvars large enough to fit mutex/condvar, corresponding
  key and inplace flag so that optimized builds don't need
  to allocate memory for objects. In debug builds with
  safe mutex enabled memory may need to be allocated for
  mutex and condvar objects.
*/

/*
  Mutex, cond and thread key arrays. The skeys arrays is
  populated during service initialization the info arrays
  are populated from after_init() hook.
 */
static std::vector<std::pair<std::string, PSI_mutex_key>>
wsrep_provider_mutex_keys;
static std::vector<PSI_mutex_info> wsrep_provider_mutexes;
static std::vector<std::pair<std::string, PSI_cond_key>>
wsrep_provider_cond_keys;
static std::vector<PSI_cond_info> wsrep_provider_conds;
static std::vector<std::pair<std::string, PSI_thread_key>>
wsrep_provider_thread_keys;
static std::vector<PSI_thread_info> wsrep_provider_threads;

/*
  Stores pthread_self()
 */
static pthread_key_t this_thread_key;


static PSI_mutex_key get_mutex_key(const wsrep::thread_service::mutex_key* key)
{
  size_t index= reinterpret_cast<size_t>(key) - 1;
  DBUG_ASSERT(index < wsrep_provider_mutex_keys.size());
  if (index >= wsrep_provider_mutex_keys.size())
  {
    WSREP_WARN("Mutex key %zu out of range", index);
    return wsrep_provider_mutex_keys[0].second;
  }
  return wsrep_provider_mutex_keys[index].second;
}

static PSI_cond_key get_cond_key(const wsrep::thread_service::cond_key* key)
{
  size_t index= reinterpret_cast<size_t>(key) - 1;
  DBUG_ASSERT(index < wsrep_provider_cond_keys.size());
  if (index >= wsrep_provider_cond_keys.size())
  {
    WSREP_WARN("Cond key %zu out of range", index);
    return wsrep_provider_cond_keys[0].second;
  }
  return wsrep_provider_cond_keys[index].second;
}

static PSI_thread_key get_thread_key(const wsrep::thread_service::thread_key* key)
{
  size_t index= reinterpret_cast<size_t>(key) - 1;
  DBUG_ASSERT(index < wsrep_provider_thread_keys.size());
  if (index >= wsrep_provider_thread_keys.size())
  {
    WSREP_WARN("Thread key %zu out of range", index);
    return wsrep_provider_thread_keys[0].second;
  }
  return wsrep_provider_thread_keys[index].second;
}




/*
  Helper class for threads.
 */
class Wsrep_instr_thread : public wsrep::thread_service::thread
{
public:
  Wsrep_instr_thread()
    : m_thread()
    , m_detached()
  { }

  int run(const wsrep::thread_service::thread_key*,
          void* (*fn)(void *), void* fn_args);

  int detach()
  {
    m_detached = true;
    return pthread_detach(m_thread);
  }

  int join(void** retval)
  {
    return pthread_join(m_thread, retval);
  }

  bool detached() const { return m_detached; }

  static Wsrep_instr_thread* self()
  {
    return reinterpret_cast<Wsrep_instr_thread*>(
                pthread_getspecific(this_thread_key));
  }

  int setschedparam(int policy, const struct sched_param* param)
  {
    return pthread_setschedparam(m_thread, policy, param);
  }

  int getschedparam(int* policy, struct sched_param* param)
  {
    return pthread_getschedparam(m_thread, policy, param);
  }

  int equal(Wsrep_instr_thread* other)
  {
    return pthread_equal(m_thread, other->m_thread);
  }
private:
  pthread_t m_thread;
  bool m_detached;
};

struct Wsrep_instr_mutex : wsrep::thread_service::mutex
{
  mysql_mutex_t mutex;
  PSI_mutex_key key;
  bool inplace;
};

struct Wsrep_instr_cond : wsrep::thread_service::cond
{
  mysql_cond_t cond;
  PSI_cond_key key;
  bool inplace;
};

struct Wsrep_thread_args
{
  Wsrep_instr_thread* this_thread;
  void* (*fn)(void*);
  void* args;
};

static void* thread_start_fn(void* args_ptr)
{
  Wsrep_thread_args* thread_args= reinterpret_cast<Wsrep_thread_args*>(args_ptr);
  Wsrep_instr_thread* this_thread= thread_args->this_thread;
  void* (*fn)(void*)= thread_args->fn;
  void* args= thread_args->args;
  delete thread_args;

  pthread_setspecific(this_thread_key, this_thread);
  void* ret= (*fn)(args);
  pthread_setspecific(this_thread_key, 0);

  if (this_thread->detached())
    delete this_thread;
  return ret;
}

extern "C"
{
  WSREP_NORETURN
  static void exit_thread(wsrep::thread_service::thread* thread,
                          void* retval)
  {
    Wsrep_instr_thread* wsrep_thread= static_cast<Wsrep_instr_thread*>(thread);
    if (wsrep_thread->detached())
    {
      delete wsrep_thread;
    }
    pthread_exit(retval);
  }
}

int Wsrep_instr_thread::run(const wsrep::thread_service::thread_key* key,
                               void* (*fn)(void *), void* fn_args)
{
  if (wsrep_provider_thread_keys.empty()) return EINVAL;
  auto thread_args= new Wsrep_thread_args{this, fn, fn_args};
  auto ret= mysql_thread_create(get_thread_key(key), &m_thread, 0,
                                thread_start_fn, thread_args);
  return ret;
}

class Wsrep_thread_service : public wsrep::thread_service
{
public:
  int before_init() override
  {
    DBUG_ASSERT(wsrep_provider_mutex_keys.size() == 0);
    DBUG_ASSERT(wsrep_provider_mutexes.size() == 0);
    DBUG_ASSERT(wsrep_provider_cond_keys.size() == 0);
    DBUG_ASSERT(wsrep_provider_conds.size() == 0);
    DBUG_ASSERT(wsrep_provider_thread_keys.size() == 0);
    DBUG_ASSERT(wsrep_provider_threads.size() == 0);
    if (wsrep_provider_mutex_keys.size() ||
        wsrep_provider_mutexes.size() ||
        wsrep_provider_cond_keys.size() ||
        wsrep_provider_conds.size() ||
        wsrep_provider_thread_keys.size() ||
        wsrep_provider_threads.size())
    {
      WSREP_ERROR("Thread service already initialized");
      return 1;
    }
    return 0;
  }

  int after_init() override
  {
    std::for_each(wsrep_provider_mutex_keys.begin(),
                  wsrep_provider_mutex_keys.end(),
                  [](std::vector<std::pair<std::string,
                     PSI_mutex_key>>::value_type& vt)
                  {
                    wsrep_provider_mutexes.push_back(
                      PSI_mutex_info{&vt.second,
                                     vt.first.data(),
                                     PSI_FLAG_GLOBAL});
                  });
    std::for_each(wsrep_provider_cond_keys.begin(),
                  wsrep_provider_cond_keys.end(),
                  [](std::vector<std::pair<std::string,
                     PSI_cond_key>>::value_type& vt)
                  {
                    wsrep_provider_conds.push_back(
                      PSI_cond_info{&vt.second,
                                    vt.first.data(),
                                    PSI_FLAG_GLOBAL});
                  });
    std::for_each(wsrep_provider_thread_keys.begin(),
                  wsrep_provider_thread_keys.end(),
                  [](std::vector<std::pair<std::string,
                     PSI_thread_key>>::value_type& vt)
                  {
                    wsrep_provider_threads.push_back(
                      PSI_thread_info{&vt.second,
                                      vt.first.data(),
                                      PSI_FLAG_GLOBAL});
                  });

    mysql_mutex_register("sql", wsrep_provider_mutexes.data(),
                         wsrep_provider_mutexes.size());
    mysql_cond_register("sql", wsrep_provider_conds.data(),
                        wsrep_provider_conds.size());
    mysql_thread_register("sql", wsrep_provider_threads.data(),
                          wsrep_provider_threads.size());
    return 0;
  }

  /*
    Threads
  */
  const thread_key* create_thread_key(const char* name)
    WSREP_NOEXCEPT override
  {
    std::string full_name("wsrep_provider_");
    full_name += name;
    wsrep_provider_thread_keys
      .push_back(std::make_pair(full_name, PSI_thread_key()));
    return reinterpret_cast<const thread_key*>(
      wsrep_provider_thread_keys.size());
  }

  int create_thread(const thread_key* key, thread** thread,
                    void* (*fn)(void*), void* fn_args) WSREP_NOEXCEPT override
  {
    auto wsrep_thread= new Wsrep_instr_thread();
    int ret= wsrep_thread->run(key, fn, fn_args);
    if (!ret)
    {
      *thread= wsrep_thread;
    }
    return ret;
  }

  int detach(thread* thread) WSREP_NOEXCEPT override
  {
    return static_cast<Wsrep_instr_thread*>(thread)->detach();
  }

  int equal(thread* left, thread* right) WSREP_NOEXCEPT override
  {
    return static_cast<Wsrep_instr_thread*>(left)->equal(
                                                   static_cast<Wsrep_instr_thread*>(right));
  }

  int join(thread* thread, void** retval) WSREP_NOEXCEPT override
  {
    Wsrep_instr_thread* wsrep_thread=
      static_cast<Wsrep_instr_thread*>(thread);
    int ret= wsrep_thread->join(retval);
    if (!ret && !wsrep_thread->detached())
    {
      delete wsrep_thread;
    }
    return ret;
  }

  thread* self() WSREP_NOEXCEPT override
  {
    return Wsrep_instr_thread::self();
  }

  int setschedparam(thread* thread, int sched,
                    const struct sched_param* sched_param)
    WSREP_NOEXCEPT override
  {
    return static_cast<Wsrep_instr_thread*>(thread)
      ->setschedparam(sched, sched_param);
  }

  int getschedparam(thread* thread, int* sched,
                    struct sched_param* sched_param)
    WSREP_NOEXCEPT override
  {
    return static_cast<Wsrep_instr_thread*>(thread)
      ->getschedparam(sched, sched_param);
  }

  /* Mutex */
  const mutex_key* create_mutex_key(const char* name) WSREP_NOEXCEPT
    override
  {
    std::string full_name("wsrep_provider_");
    full_name += name;
    wsrep_provider_mutex_keys
      .push_back(std::make_pair(full_name, PSI_mutex_key()));
    return reinterpret_cast<const mutex_key*>(
      wsrep_provider_mutex_keys.size());
  }

  mutex* init_mutex(const mutex_key* key, void* mem, size_t mem_size)
    WSREP_NOEXCEPT override
  {
    if (wsrep_provider_mutex_keys.empty()) return nullptr;
    /* @todo Use provided mem region */
    /* @todo error handling */
    Wsrep_instr_mutex* my_mutex;
    if (sizeof(Wsrep_instr_mutex) > mem_size)
    {
      my_mutex= reinterpret_cast<Wsrep_instr_mutex*>(
        my_malloc(PSI_INSTRUMENT_ME, sizeof(Wsrep_instr_mutex), MYF(0)));
      if (!my_mutex)
      {
        return nullptr;
      }
      my_mutex->inplace= false;
    }
    else
    {
      my_mutex= reinterpret_cast<Wsrep_instr_mutex*>(mem);
      my_mutex->inplace= true;
    }

    if (mysql_mutex_init(get_mutex_key(key), &my_mutex->mutex, 0))
    {
      if (!my_mutex->inplace)
      {
        my_free(my_mutex);
      }
      return nullptr;
    }

    /*
      Galera 4 does not obey strict locking order yet, which
      will cause errors in safe mutex deadlock detection.
     */
    mysql_mutex_setflags(&my_mutex->mutex, MYF_NO_DEADLOCK_DETECTION);
    return my_mutex;
  }

  int destroy(mutex* mutex) WSREP_NOEXCEPT override
  {
    auto* my_mutex= static_cast<Wsrep_instr_mutex*>(mutex);
    int ret= mysql_mutex_destroy(&my_mutex->mutex);
    if (!my_mutex->inplace)
    {
      my_free(my_mutex);
    }
    return ret;
  }

  int lock(mutex* mutex) WSREP_NOEXCEPT override
  {
    auto* my_mutex= static_cast<Wsrep_instr_mutex*>(mutex);
    return mysql_mutex_lock(&my_mutex->mutex);
  }

  int trylock(mutex* mutex) WSREP_NOEXCEPT override
  {
    auto* my_mutex= static_cast<Wsrep_instr_mutex*>(mutex);
    return mysql_mutex_trylock(&my_mutex->mutex);
  }

  int unlock(mutex* mutex) WSREP_NOEXCEPT override
  {
    auto* my_mutex= static_cast<Wsrep_instr_mutex*>(mutex);
    return mysql_mutex_unlock(&my_mutex->mutex);
  }

  /* Condition variable */
  const cond_key* create_cond_key(const char* name) WSREP_NOEXCEPT override
  {
    std::string full_name("wsrep_provider_");
    full_name += name;
    wsrep_provider_cond_keys
      .push_back(std::make_pair(full_name, PSI_cond_key()));
    return reinterpret_cast<const cond_key*>(
      wsrep_provider_cond_keys.size());
  }

  cond* init_cond(const cond_key* key, void* mem, size_t mem_size)
    WSREP_NOEXCEPT override
  {
    if (wsrep_provider_cond_keys.empty()) return nullptr;
    Wsrep_instr_cond* my_cond;
    if (sizeof(Wsrep_instr_cond) > mem_size)
    {
      my_cond= reinterpret_cast<Wsrep_instr_cond*>(
        my_malloc(PSI_INSTRUMENT_ME, sizeof(Wsrep_instr_cond), MYF(0)));
      if (!my_cond)
      {
        return nullptr;
      }
      my_cond->inplace= false;
    }
    else
    {
      my_cond= reinterpret_cast<Wsrep_instr_cond*>(mem);
      my_cond->inplace= true;
    }

    if (mysql_cond_init(get_cond_key(key), &my_cond->cond, 0))
    {
      if (!my_cond->inplace)
      {
        my_free(my_cond);
      }
      return nullptr;
    }

    return my_cond;
  }

  int destroy(cond* cond) WSREP_NOEXCEPT override
  {
    auto* my_cond= static_cast<Wsrep_instr_cond*>(cond);
    mysql_cond_destroy(&my_cond->cond);
    if (!my_cond->inplace)
    {
      my_free(my_cond);
    }
    return 0;
  }

  int wait(cond* cond, mutex* mutex) WSREP_NOEXCEPT override
  {
    auto* my_cond= static_cast<Wsrep_instr_cond*>(cond);
    auto* my_mutex= static_cast<Wsrep_instr_mutex*>(mutex);
    return mysql_cond_wait(&my_cond->cond, &my_mutex->mutex);
  }

  int timedwait(cond* cond, mutex* mutex,
                const struct timespec* ts) WSREP_NOEXCEPT
    override
  {
    auto* my_cond= static_cast<Wsrep_instr_cond*>(cond);
    auto* my_mutex= static_cast<Wsrep_instr_mutex*>(mutex);
    return mysql_cond_timedwait(&my_cond->cond, &my_mutex->mutex, ts);
  }

  int signal(cond* cond) WSREP_NOEXCEPT override
  {
    auto* my_cond= static_cast<Wsrep_instr_cond*>(cond);
    return mysql_cond_signal(&my_cond->cond);
  }

  int broadcast(cond* cond) WSREP_NOEXCEPT override
  {
    auto* my_cond= static_cast<Wsrep_instr_cond*>(cond);
    return mysql_cond_broadcast(&my_cond->cond);
  }
};

extern std::unique_ptr<Wsrep_thread_service> entrypoint;

#endif /* HAVE_PSI_THREAD_INTERFACE */

wsrep::thread_service* wsrep_thread_service_init()
{
#ifdef HAVE_PSI_THREAD_INTERFACE
  if (int err= pthread_key_create(&this_thread_key, 0))
  {
    WSREP_ERROR("Failed to create wsrep thread service this_thread_key: %s",
                strerror(err));
  }
  entrypoint = std::unique_ptr<Wsrep_thread_service>(new Wsrep_thread_service);
  entrypoint->exit= exit_thread;
  return entrypoint.get();
#else
  return nullptr;
#endif /* HAVE_PSI_THREAD_INTERFACE */
}

bool wsrep_thread_service_initialized()
{
#ifdef HAVE_PSI_THREAD_INTERFACE
  return (entrypoint != nullptr);
#else
  return false;
#endif /* HAVE_PSI_THREAD_INTERFACE */
}

void wsrep_thread_service_deinit()
{
#ifdef HAVE_PSI_THREAD_INTERFACE
  DBUG_ASSERT(wsrep_thread_service_initialized());
  entrypoint.reset();
  wsrep_provider_mutex_keys.clear();
  wsrep_provider_mutexes.clear();
  wsrep_provider_cond_keys.clear();
  wsrep_provider_conds.clear();
  wsrep_provider_thread_keys.clear();
  wsrep_provider_threads.clear();
  if (int err= pthread_key_delete(this_thread_key))
  {
    WSREP_WARN("Failed to delete wsrep thread service this_thread_key: %s",
               strerror(err));
  }
#endif /* HAVE_PSI_THREAD_INTERFACE */
}
