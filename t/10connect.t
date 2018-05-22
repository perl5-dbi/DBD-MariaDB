use strict;
use warnings;

use Test::More ;
use DBI;
use DBI::Const::GetInfoType;
$|= 1;

use vars qw($test_dsn $test_user $test_password $test_db);
use lib 't', '.';
require 'lib.pl';

my $dbh = eval { DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 1, AutoCommit => 0 }) };
if (not defined $dbh) {
    my $err = $@;
    $err =~ s/ at \S+ line \d+\.?\s*$//;
    $err = "unknown error" unless $err;
    if ( $ENV{CONNECTION_TESTING} ) {
        BAIL_OUT "no database connection: $err";
    } else {
        plan skip_all => "no database connection: $err";
    }
}

ok(defined $dbh, "Connected to database");

for my $attribute ( qw(
  mariadb_clientinfo
  mariadb_clientversion
  mariadb_serverversion
  mariadb_hostinfo
  mariadb_serverinfo
  mariadb_stat
  mariadb_protoinfo
) ) {
  ok($dbh->{$attribute}, "Value of '$attribute'");
  diag "$attribute is: ". $dbh->{$attribute};
}

my $sql_dbms_name = $dbh->get_info($GetInfoType{SQL_DBMS_NAME});
like($sql_dbms_name, qr/^MariaDB$|^MySQL$/, 'get_info SQL_DBMS_NAME is MariaDB or MySQL');
diag "SQL_DBMS_NAME is $sql_dbms_name";

my $sql_dbms_ver = $dbh->get_info($GetInfoType{SQL_DBMS_VER});
like($sql_dbms_ver, qr/^\d{2}\.\d{2}\.\d{4}$/, 'get_info SQL_DBMS_VER like ##.##.####');
diag "SQL_DBMS_VER is $sql_dbms_ver";

my $sql_server_name = $dbh->get_info($GetInfoType{SQL_SERVER_NAME});
ok(defined $sql_server_name, 'get_info SQL_SERVER_NAME');
diag "SQL_SERVER_NAME is $sql_server_name";

my $driver_ver = $dbh->get_info($GetInfoType{SQL_DRIVER_VER});
like(
  $driver_ver,
  qr/^\d{2}\.\d{2}\.\d{4}$/,
  'get_info SQL_DRIVER_VER like ##.##.####'
);
diag "SQL_DRIVER_VER is $driver_ver";

like($driver_ver, qr/^04\./, 'SQL_DRIVER_VER starts with "04." (update for 5.x)');

# The variable name for MySQL's storage engine function has varied over time
#
# From MySQL version 3.23.0 until 4.1.2: @@table_type
#   removed in 5.5.0
# https://downloads.mysql.com/docs/refman-4.1-en.a4.pdf
#
# From MySQL version 4.1.2 until 5.5.3: @@storage_engine
#   removed in 5.7.5
# http://dev.mysql.com/doc/refman/5.5/en/server-system-variables.html#sysvar_storage_engine
#
# From MySQL version 5.5.3:  @@default_storage_engine
# http://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_storage_engine

my $storage_engine = '@@default_storage_engine';
if ($dbh->{mariadb_serverversion} < 40102) {
    $storage_engine = '@@table_type';
} elsif ($dbh->{mariadb_serverversion} < 50503) {
    $storage_engine = '@@storage_engine';
}

my $result = $dbh->selectall_arrayref('select ' . $storage_engine);
my $default_storage_engine = $result->[0]->[0] || 'unknown';
diag "Default storage engine is: $default_storage_engine";

my $res = $dbh->selectrow_hashref('SELECT @@character_set_client, @@character_set_results, @@character_set_connection, @@character_set_server, @@character_set_database, @@collation_connection, @@collation_database');
foreach (sort keys %{$res}) {
    like($res->{$_}, qr/^utf8/, "Value of $_ is UTF-8");
    diag("$_ is: ". $res->{$_});
}

my $info_hashref = $dbh->{mariadb_dbd_stats};
ok(defined $info_hashref, "Value of 'mariadb_dbd_stats'");
foreach (sort keys %{$info_hashref}) {
    diag("$_ is: ". $info_hashref->{$_});
}

ok($dbh->disconnect(), 'Disconnected');

# dbi docs state:
# The username and password can also be specified using the attributes
# Username and Password, in which case they take precedence over the $username
# and $password parameters.
# see https://rt.cpan.org/Ticket/Display.html?id=89835

my $failed = not eval {$dbh= DBI->connect($test_dsn, $test_user, $test_password,
   { RaiseError => 1, PrintError => 1, AutoCommit => 0,
     Username => '4yZ73s9qeECdWi', Password => '64heUGwAsVoNqo' });};
ok($failed, 'Username and Password attributes override');

my $success = eval {$dbh= DBI->connect($test_dsn, '4yZ73s9qeECdWi', '64heUGwAsVoNqo',
   { RaiseError => 1, PrintError => 1, AutoCommit => 0,
     Username => $test_user, Password => $test_password });};
ok($success, 'Username and Password attributes override');

done_testing;
