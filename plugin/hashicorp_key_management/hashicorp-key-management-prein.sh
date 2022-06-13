# Check if we can safely upgrade.

installed=`rpm -q --whatprovides MariaDB-hashicorp-key-management 2> /dev/null`
if [ $? -eq 0 -a -n "$installed" ]; then
  installed=`echo "$installed"|sed -n 1p`
  vendor=`rpm -q --queryformat='%'"{VENDOR}" "$installed" 2>&1 | sed 's/Monty Program AB/MariaDB Foundation/'`
  version=`rpm -q --queryformat='%'"{VERSION}" "$installed" 2>&1`
  myvendor='%{mysql_vendor}'

  old_family=`echo "$version" | sed -n -e 's,^\([1-9][0-9]*\.[0-9][0-9]*\)\..*$,\1,p'`
  old_minor=`echo "$version"  | sed -n -e 's,^[1-9][0-9]*\.[0-9][0-9]*\.\([0-9][0-9]*\).*$,\1,p'`

  [ -z "$vendor" ] && vendor='<unknown>'
  [ -z "$old_family" ] && old_family="<unrecognized version: $version>"
  [ -z "$old_minor" ] && old_minor=0

  # versions from other vendors cannot be incompatible with the new plugin:
  if [ "$vendor" = "$myvendor" ]; then
    # if the old version is not 10.x, then it will not be incompatible
    # with the new key conversion algorithm:
    if [ "${old_family#10.}" != "$old_family" ]; then
      # check that the old version is younger than the one in which the
      # key conversion algorithm has changed:
      incompatible=0
      [ "$old_family" = "10.4" -a $old_minor -lt 25 ] && incompatible=1
      [ "$old_family" = "10.5" -a $old_minor -lt 16 ] && incompatible=1
      [ "$old_family" = "10.6" -a $old_minor -lt 8  ] && incompatible=1
      if [ $incompatible -ne 0 ]; then
        cat <<HERE >&2

******************************************************************
You have an old version of the MariaDB Hashicorp Key Management
plugin installed ($installed).

The new version of this plugin uses a fixed and improved key
conversion algorithm that is compatible with other plugins in
the system. Unfortunately, this new version cannot decrypt data
encrypted by the old version. Therefore, if your data is indeed
encrypted using keys stored on the Hashicorp server, then you
will need a manual upgrade procedure to install a new version
of the Hashicorp Key Management plugin.

You will need to either temporarily decrypt all data that is
encrypted by the current version of the plugin, or dump all
the data (for example, using mysqldump) in order to restore it
later, after installing a new plugin. To install a new version
of the Hashicorp plugin, you must also manually uninstall the
previous one (but after decrypting all the data encrypted with
the old version or after dumping the entire database using
mysqldump).

The sequence of actions can be something like this:

- Ensure that you have a complete, working backup of your data
  and my.cnf files;
- Dump the database with mysqldump as described in the MariaDB
  server documentation, or decrypt all the data encrypted with
  an older version of the Hashicorp Key Management plugin;
- Shut down the MariaDB server cleanly;
- Remove the existing Hashicorp Key Management plugin packages.
  Usually this command will list the packages you should remove:
  rpm -qa | grep -i '^MariaDB-hashicorp-key-management'
  You may choose to use 'rpm --nodeps -ev <package-name>' to
  remove the package containing the Hashicorp Key Management
  plugin;
- Install the new Hashicorp Key Management plugin package
  supplied by the $myvendor;
- Follow all the steps necessary to restore the database from
  the dump as described in the MariaDB server documentation or
  re-encrypt all data using the new version of the Hashicorp
  Key Management plugin.

This is a brief description of the upgrade process. Important
details can be found in the MariaDB manual (in the "Upgrading"
section).
******************************************************************
HERE
        exit 1
      fi
    fi
  fi
fi

exit 0
