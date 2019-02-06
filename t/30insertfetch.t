use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';
$|= 1;

use vars qw($test_dsn $test_user $test_password);

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

ok(defined $dbh, "Connected to database");

ok($dbh->do("CREATE TEMPORARY TABLE dbd_mysql_t30 (id INT(4), name VARCHAR(64))"), "creating table");

ok($dbh->do("
  INSERT INTO dbd_mysql_t30
    VALUES
    (1, 'Alligator Descartes'),
    (2, 'Tim Bunce')
"), "loading data");

ok(my $info = $dbh->{mariadb_info}, "mariadb_info '" . $dbh->{mariadb_info} . "'");

like($info, qr/^Records:\s\d/,   'mariadb_info: Records');
like($info, qr/Duplicates:\s0\s/, 'mariadb_info: Duplicates');
like($info, qr/Warnings: 0$/,   'mariadb_info: Warnings');

ok(
  $dbh->do("DELETE FROM dbd_mysql_t30 WHERE id IN (1,2)"),
  "deleting from table dbd_mysql_t30"
);

ok (my $sth= $dbh->prepare("SELECT * FROM dbd_mysql_t30 WHERE id = 1"));

ok($sth->execute());

ok(not $sth->fetchrow_arrayref());

ok($dbh->disconnect());

done_testing;
