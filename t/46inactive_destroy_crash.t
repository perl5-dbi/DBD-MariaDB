use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });

plan tests => 1;

# This test has to be minimal, it tries to trigger segfault which depends on order of destructed objects
# Do not modify it, adding additional ok() / pass() or removing the $socket can hide the crash
open my $socket, '+<&', $dbh->mariadb_sockfd();
ok($socket);
$dbh->{InactiveDestroy} = 1;
