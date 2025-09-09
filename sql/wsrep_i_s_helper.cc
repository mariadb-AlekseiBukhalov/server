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
#include "my_global.h"
#include "mysqld.h"
#include "sql_class.h"
#include "field.h"
#include "sql_i_s.h"
#include <stdint.h>
#include <string>
#include "wsrep_i_s_helper.h"

/* Helper to store longlong or NULL */
int store_longlong(Field *field, const long long val)
{
  if (val < 0)
  {
    field->set_null();
    return 0;
  }

  field->set_notnull();
  return field->store(val, false);
}

/* Helper to store uint32_t or NULL if val == (uint32_t)(-1)*/
int store_ulong(Field *field, const uint32_t val)
{
  if (val == (uint32_t)(-1))
  {
    field->set_null();
    return 0;
  }

  field->set_notnull();
  return field->store(val, false);
}

/* Helper to store string or NULL */
int store_string(Field* field, const std::string &str)
{
  if (str.empty())
  {
    field->set_null();
    return 0;
  }

  field->set_notnull();
  return field->store(str.c_str(), uint(str.size()), system_charset_info);
}

/* Helper to store ulonglong to DATETIME */
int store_time(Field* field, const my_hrtime_t hrtime)
{
  MYSQL_TIME	my_time;
  struct tm	tm_time;
  time_t        time;

  time= hrtime_to_time(hrtime);
  localtime_r(&time, &tm_time);
  localtime_to_TIME(&my_time, &tm_time);
  my_time.time_type = MYSQL_TIMESTAMP_DATETIME;
  my_time.second_part= hrtime_sec_part(hrtime);
  return(field->store_time_dec(&my_time, 6));
}
