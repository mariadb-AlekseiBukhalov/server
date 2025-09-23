if (`SELECT COUNT(*) = 1 FROM information_schema.global_variables WHERE VARIABLE_NAME = "wsrep_provider" AND VARIABLE_VALUE = "grp"`)
{
  --skip $grp_skip_reason
}
