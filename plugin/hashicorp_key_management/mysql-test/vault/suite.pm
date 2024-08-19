package My::Suite::Vault;
use My::Platform;

@ISA = qw(My::Suite);

use strict;


if (IS_WINDOWS) {
  # On Windows, use 'where' to check for vault binary
  return "Hashicorp Vault key management utility not found"
    unless `where vault 2> NUL`;
} else {
  # On Unix, use 'command -v'
  return "Hashicorp Vault key management utility not found"
    unless `sh -c "command -v vault"`;
}

return "You need to set the value of the VAULT_ADDR variable"
  unless $ENV{VAULT_ADDR};

return "You need to set the value of the VAULT_TOKEN variable"
  unless $ENV{VAULT_TOKEN};

sub check_vault_enterprise_namespaces {
  my $status = `vault status 2>&1`;
  if ($? >> 8 != 0) {
     return 0;
  }
  if ($status =~ /Version\s*([\d\.]+.*)(\+ent)/i) {
     my $output = `vault namespace list 2>&1`;
     if ($? >> 8 != 0 && $output !~ /No namespaces found/i) {
       return 0;
     }
  }
  else {
    return 0;
  }
  return 1;
}

sub skip_combinations {
  my %skip;
  my @combinations;
  $skip{'include/have_namespaces.inc'} = 'Hashicorp server with namespaces support is required'
            unless check_vault_enterprise_namespaces();
  %skip;
}

bless {};
