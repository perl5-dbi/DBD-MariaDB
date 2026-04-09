use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });

if (not eval { my $tmp = $dbh->{mariadb_max_allowed_packet}; 1 }) {
    plan skip_all => $dbh->errstr();
}

plan tests => 5;

$dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1, mariadb_max_allowed_packet => 8192*2 });
is $dbh->{mariadb_max_allowed_packet}, 8192*2, 'mariadb_max_allowed_packet is correct';
ok !defined eval { $dbh->do(q{SELECT '} . ("X" x (8192*2)) . q{'}) }, 'Statement bigger then maximal packet size is not accepted';
like $dbh->err(), qr/^20(?:13|20)$/, 'Error code for bigger statement is CR_SERVER_LOST (2013) or CR_NET_PACKET_TOO_LARGE (2020)';

$dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1, mariadb_max_allowed_packet => 8192*2 });
is $dbh->{mariadb_max_allowed_packet}, 8192*2, 'mariadb_max_allowed_packet is correct';
ok eval { $dbh->do(q{SELECT '} . ("X" x (8192)) . q{'}) }, 'Statement smaller then maximal packet size is accepted';
