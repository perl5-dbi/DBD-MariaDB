use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

plan skip_all => "Connection to Embedded server does not have socket" if $dbh->{mariadb_hostinfo} eq 'Embedded';

plan tests => 4;

my $fd = $dbh->mariadb_sockfd;
ok((defined $fd && $fd >= 0), '$dbh->mariadb_sockfd returns valid file descriptor when $dbh connection is open');
$fd = $dbh->{mariadb_sockfd};
ok((defined $fd && $fd >= 0), '$dbh->{mariadb_sockfd} returns valid file descriptor when $dbh connection is open');

$dbh->disconnect;

ok(!defined $dbh->mariadb_sockfd, '$dbh->mariadb_sockfd returns undef when $dbh connection was closed');
ok(!defined $dbh->{mariadb_sockfd}, '$dbh->{mariadb_sockfd} returns undef when $dbh connection was closed');
