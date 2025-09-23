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

#ifndef WSREP_GALERA_REPLICATION_H
#define WSREP_GALERA_REPLICATION_H

/*
  Galera replication plugin function accessors to be used from WSREP.
*/

bool wsrep_is_galera_plugin_loaded();

struct st_mysql_galera_replication;
/**
   Return galera plugin descriptor.
 */
st_mysql_galera_replication *wsrep_get_galera_plugin_descriptor();

/**
   Return textual information about the plugin.
 */
const char *wsrep_get_galera_plugin_name();
const char *wsrep_get_galera_plugin_author();
const char *wsrep_get_galera_plugin_version_info();

#endif // WSREP_GALERA_REPLICATION_H
