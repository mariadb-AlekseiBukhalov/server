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
  Implementation of wsrep provider threads instrumentation.
 */

#ifndef WSREP_PROVIDER_THREADS_H
#define WSREP_PROVIDER_THREADS_H

#include "wsrep/thread_service.hpp"

wsrep::thread_service* wsrep_thread_service_init();

bool wsrep_thread_service_initialized();

void wsrep_thread_service_deinit();

#endif /* WSREP_PROVIDER_THREADS_H */
