if (`SELECT COUNT(*) = 0 FROM information_schema.global_variables WHERE VARIABLE_NAME = 'wsrep_provider' AND VARIABLE_VALUE = 'grp'`)
{
  --skip "Test requires Galera Plugin"
}