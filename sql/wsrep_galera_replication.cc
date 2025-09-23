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

#include "wsrep_galera_replication.h"

#include <cstdlib>

#include <my_global.h>
#include <m_string.h>
#include <mysql/plugin_galera_replication.h>

#include "sql_plugin.h"

namespace
{

/**
  Static name of Galera replication plugin.
*/
LEX_CSTRING galera_replication_plugin_name= {
    STRING_WITH_LEN("galera_replication")};

struct Wsrep_galera_plugin
{
  Wsrep_galera_plugin(THD *thd= nullptr) : m_thd{thd}
  {
    m_plugin= my_plugin_lock_by_name(m_thd, &galera_replication_plugin_name,
                                     MYSQL_REPLICATION_PLUGIN);
  }

  ~Wsrep_galera_plugin()
  {
    if (m_plugin)
    {
      plugin_unlock(m_thd, m_plugin);
    }
  }

  bool loaded() const { return m_plugin; }

  st_maria_plugin *decl() { return plugin_decl(m_plugin); }

private:
  THD *m_thd;
  plugin_ref m_plugin;
};

} // namespace

bool wsrep_is_galera_plugin_loaded()
{
  Wsrep_galera_plugin plugin;
  return plugin.loaded();
}

st_mysql_galera_replication *wsrep_get_galera_plugin_descriptor()
{
  Wsrep_galera_plugin plugin;
  return (st_mysql_galera_replication *) plugin.decl()->info;
}

const char *wsrep_get_galera_plugin_name()
{
  Wsrep_galera_plugin plugin;
  return plugin.decl()->name;
}

const char *wsrep_get_galera_plugin_author()
{
  Wsrep_galera_plugin plugin;
  return plugin.decl()->author;
}

const char *wsrep_get_galera_plugin_version_info()
{
  Wsrep_galera_plugin plugin;
  return plugin.decl()->version_info;
}
