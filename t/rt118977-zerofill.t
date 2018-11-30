use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 1, RaiseError => 1 });

plan tests => 6*2;

for my $mariadb_server_prepare (0, 1) {
	$dbh->{mariadb_server_prepare} = $mariadb_server_prepare;
	ok $dbh->do("CREATE TEMPORARY TABLE t(id int(5) unsigned zerofill)");
	ok $dbh->do("INSERT INTO t(id) VALUES(1)");
	ok $dbh->do("INSERT INTO t(id) VALUES(1234567890)");
	is $dbh->selectcol_arrayref("SELECT id FROM t")->[0], "00001";
	is $dbh->selectcol_arrayref("SELECT id FROM t")->[1], "1234567890";
	ok $dbh->do("DROP TEMPORARY TABLE t");
}
