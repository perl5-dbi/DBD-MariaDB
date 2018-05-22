use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });

plan tests => 4;

$dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1, mariadb_max_allowed_packet => 8192*2 });
is $dbh->{mariadb_max_allowed_packet}, 8192*2, 'mariadb_max_allowed_packet is correct';
ok !defined eval { $dbh->do('SELECT "' . ("X" x (8192*2)) . '"') }, 'Statement bigger then maximal packet size is not accepted';

$dbh = DBI->connect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1, mariadb_max_allowed_packet => 8192*2 });
is $dbh->{mariadb_max_allowed_packet}, 8192*2, 'mariadb_max_allowed_packet is correct';
ok eval { $dbh->do('SELECT "' . ("X" x (8192)) . '"') }, 'Statement smaller then maximal packet size is accepted';
