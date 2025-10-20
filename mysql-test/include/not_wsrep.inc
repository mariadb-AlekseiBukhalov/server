# To be used in a test which requires server to be compiled with no wsrep support
# (-DWITH_WSREP=OFF) and wsrep plugin is DISABLED.

if (`SELECT COUNT(*)=1 FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'wsrep' AND PLUGIN_STATUS='ACTIVE'`)
{
  --skip Test requires no wsrep plugin.
}

