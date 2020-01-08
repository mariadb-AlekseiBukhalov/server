# Copyright (c) 2018, Codership Oy. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

CMAKE_MINIMUM_REQUIRED(VERSION 3.12.0)

INCLUDE_DIRECTORIES("." "../include")

SET(BLACKBOX_SOURCES blackbox.c)

ADD_LIBRARY(blackbox ${BLACKBOX_SOURCES})

ADD_EXECUTABLE(mariadb-bbtool mariadb-bbtool.c)
TARGET_LINK_LIBRARIES(mariadb-bbtool blackbox)
TARGET_LINK_LIBRARIES(mariadb-bbtool "-lrt")
