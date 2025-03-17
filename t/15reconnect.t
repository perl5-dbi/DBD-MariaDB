use strict;
use warnings;

use Test::More;
use DBI;
$|= 1;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh;
my $sth;

# Test connection
$dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, PrintError => 0 });
$dbh->disconnect();

plan tests => 30 * 2;

for my $mariadb_server_prepare (0, 1) {
$dbh= DBI->connect("$test_dsn;mariadb_server_prepare=$mariadb_server_prepare;mariadb_server_prepare_disable_fallback=1", $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

ok(defined $dbh, "Connected to database");

ok($dbh->{Active}, "checking for active handle");

ok($dbh->{mariadb_auto_reconnect} = 1, "enabling reconnect");

ok($dbh->{AutoCommit} = 1, "enabling autocommit");

ok($dbh->disconnect(), "disconnecting active handle");

ok(!$dbh->{Active}, "checking for inactive handle");

ok($dbh->do("SELECT 1"), "implicitly reconnecting handle with 'do'");

ok($dbh->{Active}, "checking for reactivated handle");

SKIP: {

skip "Connection to Embedded server does not have socket", 3 if $dbh->{mariadb_hostinfo} eq 'Embedded';

ok(shutdown_mariadb_socket($dbh), "shutdown socket handle");

ok($dbh->do("SELECT 1"), "implicitly reconnecting handle after shutdown with 'do'");

ok($dbh->{Active}, "checking for reactivated handle");

}

ok($dbh->disconnect(), "disconnecting active handle");

ok(!$dbh->{Active}, "checking for inactive handle");

ok($sth = $dbh->prepare("SELECT 1"), "implicitly reconnecting handle with preparing statement");

ok($sth->execute(), "execute prepared statement");

ok($dbh->{Active}, "checking for reactivated handle");

$sth->finish();

ok($sth = $dbh->prepare("SELECT 1"), "prepare statement");

ok($dbh->disconnect(), "disconnecting active handle");

ok(!$dbh->{Active}, "checking for inactive handle");

ok($sth->execute(), "implicitly reconnecting handle with executing prepared statement");

ok($dbh->{Active}, "checking for reactivated handle");

$sth->finish();

ok($dbh->do("SET SESSION wait_timeout=1"), "set wait_timeout to 1");

note("call perl sleep 2 for implicit disconnect due to wait_timeout");
sleep(2);

ok($sth = $dbh->prepare("SELECT 1"), "implicitly reconnecting handle with preparing statement");

ok($sth->execute(), "execute prepared statement");

ok($dbh->{Active}, "checking for reactivated handle");

ok($sth = $dbh->prepare("SELECT 1"), "prepare statement");

note("call perl sleep 2 for implicit disconnect due to wait_timeout");
sleep(2);

ok($sth->execute(), "implicitly reconnecting handle with executing prepared statement");

ok($dbh->{Active}, "checking for reactivated handle");

$sth->finish();

note("call perl sleep 2 for implicit disconnect due to wait_timeout");
sleep(2);

ok($dbh->do("SELECT 1"), "implicitly reconnecting handle with 'do'");

ok($dbh->{Active}, "checking for reactivated handle");

$dbh->disconnect();
}
