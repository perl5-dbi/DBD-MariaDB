use strict;
use warnings;

use Test::More;
use DBI;
$|= 1;

my $rows = 0;
my $sth;
my $testInsertVals;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });
plan tests => 124;

ok(defined $dbh, "Connected to database");

ok($dbh->do("DROP TABLE IF EXISTS dbd_mysql_t35"), "making slate clean");

ok($dbh->do("CREATE TABLE dbd_mysql_t35 (id INT(4), name VARCHAR(64), name_limit INT, limit_by INT)"), "creating table");

ok(($sth = $dbh->prepare("INSERT INTO dbd_mysql_t35 VALUES (?,?,?,?)")));

for my $i (0..99) {
  my @chars = grep !/[0O1Iil]/, 0..9, 'A'..'Z', 'a'..'z';
  my $random_chars = join '', map { $chars[rand @chars] } 0 .. 16;
  my $random_num = int(rand(1000));

  # save these values for later testing
  $testInsertVals->{$i} = $random_chars;
  ok(($rows = $sth->execute($i, $random_chars, $random_num, $random_num)));
}

ok($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t35 LIMIT ?, ?"),
  'testing prepare of select statement with LIMIT placeholders');

ok($sth->execute(20, 50), 'testing exec of bind vars for limit');

my ($array_ref);
ok(defined($array_ref = $sth->fetchall_arrayref));
ok(!$sth->err);

is(scalar @$array_ref, 50);

ok($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t35 WHERE limit_by > ?"),
  "testing prepare of select statement with started by 'limit' column");

ok($sth->execute(-1), 'testing exec of bind vars for placeholder');

ok(defined($array_ref = $sth->fetchall_arrayref));
ok(!$sth->err);

is(scalar @$array_ref, 100);

ok($dbh->do("UPDATE dbd_mysql_t35 SET name_limit = ? WHERE id = ?", undef, 128, 1));

ok($dbh->do("UPDATE dbd_mysql_t35 SET name = ? WHERE name_limit > ?", undef, "updated_string", 999999));

# newline before LIMIT
ok($dbh->do(<<'SQL'
UPDATE dbd_mysql_t35 SET name = ?
LIMIT ?
SQL
, undef, "updated_string", 0));

# tab before LIMIT
ok($dbh->do(<<'SQL'
	UPDATE dbd_mysql_t35 SET name = ?
	LIMIT ?
SQL
, undef, "updated_string", 0));

ok($dbh->do("DROP TABLE dbd_mysql_t35"));

# Issue #205: A column named "limits" shouldn't be parsed as LIMIT.
my $limits = 500;
my $flag = 1;
my $id = 1;
$dbh->do('CREATE TABLE IF NOT EXISTS dbd_mysql_t35_1 ( id INT(10) PRIMARY KEY, lxmxts INT(10), flag ENUM("9","0","1") )');
$dbh->do('INSERT INTO dbd_mysql_t35_1 SET id=?, lxmxts=?, flag=?', undef, $id, $limits, $flag);
my ($set_flag1) = $dbh->selectrow_array('SELECT flag FROM dbd_mysql_t35_1 WHERE id=?', undef, $id);

is($set_flag1, $flag, 'flag set without limits involved');

ok($dbh->do('DROP TABLE dbd_mysql_t35_1'));

$dbh->do('CREATE TABLE IF NOT EXISTS dbd_mysql_t35_2 ( id INT(10) PRIMARY KEY, limits INT(10), flag ENUM("9","0","1") )');
$dbh->do('INSERT INTO dbd_mysql_t35_2 SET id=?, limits=?, flag=?', undef, $id, $limits, $flag);
my ($set_flag2) = $dbh->selectrow_array('SELECT flag FROM dbd_mysql_t35_2 WHERE id=?', undef, $id);

is($set_flag2, $flag, 'flag set with limits involved');

ok($dbh->do('DROP TABLE dbd_mysql_t35_2'));

ok($dbh->disconnect);
