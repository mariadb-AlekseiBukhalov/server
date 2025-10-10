#
# Wait until a connection is fully ready for use
#
# Usage example:
#   connect (conn1,localhost,root,,);
#   --source include/wait_until_connected_sync.inc
#
# This script ensures the new connection is usable before continuing.
#

--disable_query_log
--disable_result_log

let $counter=5000;
let $mysql_errno=9999;

while ($mysql_errno)
{
  --error 0,ER_ACCESS_DENIED_ERROR,ER_SERVER_SHUTDOWN,ER_CONNECTION_KILLED,ER_LOCK_WAIT_TIMEOUT,2002,2006,2013
  SELECT 1;

  dec $counter;
  if (!$counter)
  {
    --die Connection failed to become ready in time
  }

  # If SELECT succeeded, exit the loop
  if (!$mysql_errno)
  {
    let $mysql_errno=0;
  }

  if ($mysql_errno == 1045)
  {
    let $mysql_errno=0;
  }

  if ($mysql_errno > 0)
  {
    --sleep 0.1
  }
}

--enable_query_log
--enable_result_log
