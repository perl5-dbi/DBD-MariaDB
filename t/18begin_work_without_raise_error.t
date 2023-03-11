use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 0, PrintError => 1 });

plan tests => 12;

my $warn;
$SIG{__WARN__} = sub { $warn = $_[0]; note "Caught DBI warn: $warn"; };

my $error;
$SIG{__DIE__} = sub { $error = $_[0]; note "Caught DBI error: $error"; };

# Simulate network failure for this test by calling shutdown() on the MariaDB socket
# This check that begin_work throw error on failure also when RaiseError is turned off
ok(shutdown_mariadb_socket($dbh), "shutdown socket handle");

ok(!$dbh->{RaiseError}, "RaiseError is not set");

$error = undef;
ok(!eval { $dbh->begin_work() }, "begin_work failed");
like($error, qr/Changing AutoCommit attribute failed/, "begin_work croaked");
ok($dbh->err(), "err is set");
ok(length($dbh->errstr()), "errstr is set");

ok(!$dbh->do('SELECT 1'), "do failed");
ok($dbh->err(), "err is set");
ok(length($dbh->errstr()), "errstr is set");

$warn = undef;
ok(!$dbh->commit(), "commit failed");
like($warn, qr/commit ineffective with AutoCommit enabled/, "commit threw warning");

ok($dbh->disconnect(), "disconnect");
