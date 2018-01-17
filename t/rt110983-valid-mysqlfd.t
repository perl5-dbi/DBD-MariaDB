use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, AutoCommit => 0 });

plan tests => 4;

ok($dbh->mariadb_fd >= 0, '$dbh->mariadb_fd returns valid file descriptor when $dbh connection is open');
ok($dbh->{sockfd} >= 0, '$dbh->{sockfd} returns valid file descriptor when $dbh connection is open');

$dbh->disconnect;

ok(!defined $dbh->mariadb_fd, '$dbh->mariadb_fd returns undef when $dbh connection was closed');
ok(!defined $dbh->{sockfd}, '$dbh->{sockfd} returns undef when $dbh connection was closed');
