#
# Copyright (C) 2021-2022 Codership Oy <info@codership.com>
# Copyright (C) 2025 MariaDB Corp
#

if (NOT PLUGIN_GALERA_REPLICATION OR PLUGIN_GALERA_REPLICATION STREQUAL "NO")
  ADD_FEATURE_INFO(GALERA_REPLICATION "OFF" "Galera Plugin/CRAFT")
  return()
endif()

message(STATUS "Galera Plugin Stub")
MYSQL_ADD_PLUGIN(galera_replication galera_replication.cc
  MODULE_ONLY RECOMPILE_FOR_EMBEDDED)
