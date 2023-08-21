use strict;
use warnings;

use Test::More;
use DBI;
use DBI::Const::GetInfoType;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });

plan tests => 2;

for my $mariadb_server_prepare (0, 1) {
	$dbh->{mariadb_server_prepare} = $mariadb_server_prepare;
	SKIP: {
	skip 'MySQL 8.0.22+ server have broken ROUND() function', 1 if $dbh->get_info($GetInfoType{SQL_DBMS_NAME}) eq 'MySQL' and $dbh->{mariadb_serverversion} >= 80022;
	is $dbh->selectrow_arrayref('SELECT round(degrees(0.00043) * 69, 2)')->[0], '1.70',
		'floats with fixed-length of decimals returns correct value for mariadb_server_prepare=' . $mariadb_server_prepare;
	}
}
