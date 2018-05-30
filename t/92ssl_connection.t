use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });
my $have_ssl = eval { $dbh->selectrow_hashref("SHOW VARIABLES WHERE Variable_name = 'have_ssl'") };
$dbh->disconnect();
plan skip_all => 'Server does not support SSL connections' unless $have_ssl and $have_ssl->{Value} eq 'YES';

plan tests => 2;

$dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 0, mariadb_ssl => 1 });
if (defined $dbh) {
  pass('DBD::MariaDB supports mariadb_ssl=1 without mariadb_ssl_optional=1 and connect to server');
  ok(defined $dbh->{mariadb_ssl_cipher}, 'SSL connection was established') and diag("mariadb_ssl_cipher is: ". $dbh->{mariadb_ssl_cipher});
} else {
  like($DBI::errstr, qr/^SSL connection error: /, 'DBD::MariaDB supports mariadb_ssl=1 without mariadb_ssl_optional=1 and fail because cannot enforce SSL encryption') or diag('Error message: ' . ($DBI::errstr || 'unknown'));
  is($DBI::err, 2026, 'DBD::MariaDB error code is SSL related') or diag('Error code: ' . ($DBI::err || 'unknown'));
}
