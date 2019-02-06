use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';
use utf8;

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

plan skip_all => "no support for utf8mb4"
    unless $dbh->selectrow_array("SHOW CHARSET LIKE 'utf8mb4'");

ok $dbh->do("CREATE TEMPORARY TABLE dbd_mysql_t55utf8mb4 (id SERIAL, val TEXT)");

my $sth = $dbh->prepare("INSERT INTO dbd_mysql_t55utf8mb4(val) VALUES('😈')");
$sth->execute();

my $query = "SELECT val, HEX(val) FROM dbd_mysql_t55utf8mb4 LIMIT 1";
$sth = $dbh->prepare($query) or die "$DBI::errstr";
ok $sth->execute;

ok(my $ref = $sth->fetchrow_arrayref, 'fetch row');
cmp_ok $ref->[0], 'eq', "😈", 'test U+1F608';
cmp_ok $ref->[1], 'eq', "F09F9888";

$dbh->disconnect();
done_testing;
