/* Copyright (C) 2025 Codership Oy <info@galeracluster.com>

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; either version 2
   of the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
   USA.
*/

#ifndef WSREP_I_S_HELPER_H
#define WSREP_I_S_HELPER_H 1

/** Max length of varchar fields */
#define WSREP_THD_INFO_LEN 32
#define WSREP_CMP_SCHEME_LEN 3
#define WSREP_CMP_BIG_LEN 256
#define WSREP_BFA_LEN 128

/** Helper to store longlong or NULL */
int store_longlong(Field *field, const long long val);

/** Helper to store uint32_t or NULL if val == (uint32_t)(-1)*/
int store_ulong(Field *field, const uint32_t val);

/** Helper to store string or NULL */
int store_string(Field* field, const std::string &str);

/** Helper to store ulonglong to DATETIME */
int store_time(Field* field, const my_hrtime_t hrtime);

/** Helper to check return value */
#define WSREP_IS_OK(expr)     \
  if ((expr) != 0)   \
  {                  \
    return (1);      \
  }

#endif // WSREP_I_S_HELPER_H
