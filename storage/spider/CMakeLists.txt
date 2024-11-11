IF(HAVE_WVLA)
  SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-vla")
  SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wno-vla")
ENDIF()

IF(MSVC)
  # Temporarily disable "conversion from size_t .."
  IF(CMAKE_SIZEOF_VOID_P EQUAL 8)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4267")
  ENDIF()
ENDIF()

SET(SPIDER_SOURCES
  spd_param.cc spd_sys_table.cc spd_trx.cc spd_db_conn.cc spd_conn.cc
  spd_table.cc spd_direct_sql.cc spd_udf.cc spd_ping_table.cc
  spd_copy_tables.cc spd_i_s.cc spd_malloc.cc ha_spider.cc spd_udf.def
  spd_db_mysql.cc spd_group_by_handler.cc spd_db_include.cc
  spd_db_odbc.cc spd_db_odbc_mariadb.cc
)

set(CPACK_RPM_spider-engine_PACKAGE_SUMMARY "Spider storage engine for MariaDB" PARENT_SCOPE)
set(CPACK_RPM_spider-engine_PACKAGE_DESCRIPTION " The Spider storage engine with built-in sharding features. It supports
 partitioning and xa transactions, and allows tables of different MariaDB
 instances to be handled as if they were on the same instance. It refers to one
 possible implementation of ISO/IEC 9075-9:2008 SQL/MED." PARENT_SCOPE)

FIND_PATH(UNIXODBC_INCLUDE_DIR sql.h sqlext.h PATHS /usr/include)
FIND_LIBRARY(UNIXODBC_LIBRARY NAMES odbc PATHS /usr/lib64)

IF(PLUGIN_PARTITION MATCHES "^NO$")
  MESSAGE(STATUS "Spider is skipped because partitioning is disabled")
ELSE()

  MYSQL_ADD_PLUGIN(spider ${SPIDER_SOURCES}
    STORAGE_ENGINE MODULE_ONLY COMPONENT spider-engine)
  IF(NOT TARGET spider)
    RETURN()
  ENDIF()
ENDIF()

IF(UNIXODBC_INCLUDE_DIR AND UNIXODBC_LIBRARY)
  SET(SPIDER_WITH_UNIXODBC ON CACHE BOOL "Spider is compiled with unixODBC library.")
  IF(SPIDER_WITH_UNIXODBC)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHAVE_SPIDER_UNIXODBC")
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DHAVE_SPIDER_UNIXODBC")
    INCLUDE_DIRECTORIES(${UNIXODBC_INCLUDE_DIR})
    TARGET_LINK_LIBRARIES (spider ${UNIXODBC_LIBRARY})
  ENDIF()
ENDIF()
