-- let $sr = `SELECT variable_value LIKE '%:STREAMING:%' FROM information_schema.session_status WHERE variable_name = 'wsrep_provider_capabilities'`

if (!$sr)
{
  -- skip The test requires a wsrep provider that supports streaming replication.
}
