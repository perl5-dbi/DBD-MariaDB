use strict;
use warnings;

use Test::More;
use DBI;
use lib 't', '.';
require 'lib.pl';

my ($row, $sth, $dbh);
my ($def, $rows, $ret_ref);
use vars qw($test_dsn $test_user $test_password);

$dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
    { RaiseError => 1, PrintError => 0 });

plan tests => 5+2*63;

for my $mariadb_server_prepare (0, 1) {

$dbh->{mariadb_server_prepare} = $mariadb_server_prepare;

ok($dbh->do("DROP TABLE IF EXISTS dbd_mysql_t35prepare"), "Making slate clean");

ok($dbh->do("CREATE TABLE dbd_mysql_t35prepare (id INT(4), name VARCHAR(64))"),
  "Creating table");

ok($sth = $dbh->prepare("SHOW TABLES LIKE 'dbd_mysql_t35prepare'"),
  "Testing prepare show tables");

ok($sth->execute(), "Executing 'show tables'");

ok((defined($row= $sth->fetchrow_arrayref) &&
  (!$sth->err)),
  "Testing if result set and no errors");

ok($row->[0] eq 'dbd_mysql_t35prepare', "Checking if results equal to 'dbd_mysql_t35prepare' \n");

ok($sth->finish, "Finishing up with statement handle");

ok($dbh->do("INSERT INTO dbd_mysql_t35prepare VALUES (1,'1st first value')"),
  "Inserting first row");

ok($sth= $dbh->prepare("INSERT INTO dbd_mysql_t35prepare VALUES (2,'2nd second value')"),
  "Preparing insert of second row");

ok(($rows = $sth->execute()), "Inserting second row");

ok($rows == 1, "One row should have been inserted");

ok($sth= $dbh->prepare("SELECT id, name FROM dbd_mysql_t35prepare WHERE id = 1"),
  "Testing prepare of query");

ok($sth->execute(), "Testing execute of query");

ok($ret_ref = $sth->fetchall_arrayref(),
  "Testing fetchall_arrayref of executed query");

ok($sth= $dbh->prepare("INSERT INTO dbd_mysql_t35prepare values (?, ?)"),
  "Preparing insert, this time using placeholders");

my $testInsertVals = {};
for (my $i = 0 ; $i < 10; $i++)
{
  my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
  my $random_chars= join '', map { $chars[rand @chars] } 0 .. 16;
   # save these values for later testing
  $testInsertVals->{$i}= $random_chars;
  ok($rows= $sth->execute($i, $random_chars), "Testing insert row");
  is($rows, 1, "Should have inserted one row");
}

ok($sth= $dbh->prepare("SELECT * FROM dbd_mysql_t35prepare WHERE id = ? OR id = ?"),
  "Testing prepare of query with placeholders");

ok($rows = $sth->execute(1,2),
  "Testing execution with values id = 1 or id = 2");

ok($ret_ref = $sth->fetchall_arrayref(),
  "Testing fetchall_arrayref (should be four rows)");

note "RETREF " . scalar @$ret_ref . "\n";
ok(@{$ret_ref} == 4 , "\$ret_ref should contain four rows in result set");

# Check that repeated $sth->execute + $sth->fetchall_arrayref work as expected
ok($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t35prepare LIMIT 2"));
ok($sth->execute());
is_deeply($sth->fetchall_arrayref(), [ [ 1, '1st first value' ], [ 2, '2nd second value' ] ]);
ok($sth->execute());
is_deeply($sth->fetchall_arrayref(), [ [ 1, '1st first value' ], [ 2, '2nd second value' ] ]);
ok($sth->execute());
is_deeply($sth->fetchall_arrayref(), [ [ 1, '1st first value' ], [ 2, '2nd second value' ] ]);

# Check that repeated $sth->execute + $sth->fetchrow_arrayref work as expected
ok($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t35prepare LIMIT 3"));
ok($sth->execute());
is_deeply($sth->fetchrow_arrayref(), [ 1, '1st first value' ]);
is_deeply($sth->fetchrow_arrayref(), [ 2, '2nd second value' ]);
ok($sth->finish());
ok($sth->execute());
is_deeply($sth->fetchrow_arrayref(), [ 1, '1st first value' ]);
is_deeply($sth->fetchrow_arrayref(), [ 2, '2nd second value' ]);
ok($sth->finish());

# Check that repeated calls of $dbh->selectcol_arrayref, $dbh->prepare and $dbh->prepare_cached work as expected
is_deeply($dbh->selectcol_arrayref("SELECT id FROM dbd_mysql_t35prepare LIMIT 2"), [ 1, 2 ]);
is_deeply($dbh->selectcol_arrayref("SELECT id FROM dbd_mysql_t35prepare LIMIT 2"), [ 1, 2 ]);
is_deeply($dbh->selectcol_arrayref($dbh->prepare("SELECT id FROM dbd_mysql_t35prepare LIMIT 2")), [ 1, 2 ]);
is_deeply($dbh->selectcol_arrayref($dbh->prepare("SELECT id FROM dbd_mysql_t35prepare LIMIT 2")), [ 1, 2 ]);
is_deeply($dbh->selectcol_arrayref($dbh->prepare_cached("SELECT id FROM dbd_mysql_t35prepare LIMIT 2")), [ 1, 2 ]);
is_deeply($dbh->selectcol_arrayref($dbh->prepare_cached("SELECT id FROM dbd_mysql_t35prepare LIMIT 2")), [ 1, 2 ]);

ok($sth= $dbh->prepare("DROP TABLE IF EXISTS dbd_mysql_t35prepare"),
  "Testing prepare of dropping table");

ok($sth->execute(), "Executing drop table");

}

# Bug #20153: Fetching all data from a statement handle does not mark it
# as finished
ok($sth= $dbh->prepare("SELECT 1"), "Prepare - Testing bug #20153");
ok($sth->execute(), "Execute - Testing bug #20153");
ok($sth->fetchrow_arrayref(), "Fetch - Testing bug #20153");
ok(!($sth->fetchrow_arrayref()),"Not Fetch - Testing bug #20153");

# Install a handler so that a warning about unfreed resources gets caught
$SIG{__WARN__} = sub { die @_ };

ok($dbh->disconnect(), "Testing disconnect");
