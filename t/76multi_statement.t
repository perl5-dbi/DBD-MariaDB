use strict;
use warnings;

use Test::More;
use DBI;
use DBD::MariaDB;
use lib 't', '.';
require 'lib.pl';
$|= 1;

use vars qw($test_dsn $test_user $test_password);

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0,
                        mariadb_multi_statements => 1 });

plan tests => 71;

ok (defined $dbh, "Connected to database with multi statement support");

$dbh->{mariadb_server_prepare}= 0;

  ok($dbh->do("SET SQL_MODE=''"),"init connection SQL_MODE non strict");

  ok($dbh->do("DROP TABLE IF EXISTS dbd_mysql_t76multi"), "clean up");

  ok($dbh->do("CREATE TABLE dbd_mysql_t76multi (a INT)"), "create table");

  ok($dbh->do("INSERT INTO dbd_mysql_t76multi VALUES (1); INSERT INTO dbd_mysql_t76multi VALUES (2);"), "2 inserts");

   # Check that a second do() doesn't fail with an 'Out of sync' error
  ok($dbh->do("INSERT INTO dbd_mysql_t76multi VALUES (3); INSERT INTO dbd_mysql_t76multi VALUES (4);"), "2 more inserts");

  # Check that more_results works for non-SELECT results too
  my $sth;
  ok($sth = $dbh->prepare("UPDATE dbd_mysql_t76multi SET a=5 WHERE a=1; UPDATE dbd_mysql_t76multi SET a='6suffix' WHERE a<4"));
  ok($sth->execute(), "Execute updates");
  is($sth->rows, 1, "First update affected 1 row");
  is($sth->{mariadb_warning_count}, 0, "First update had no warnings");
  ok($sth->{Active}, "Statement handle is Active");
  ok($sth->more_results());
  is($sth->rows, 2, "Second update affected 2 rows");
  is($sth->{mariadb_warning_count}, 2, "Second update had 2 warnings");
  ok(not $sth->more_results());

  # Now run it again without calling more_results().
  ok($sth->execute(), "Execute updates again");
  ok($sth->finish());

  # Check that do() doesn't fail with an 'Out of sync' error
  is($dbh->do("DELETE FROM dbd_mysql_t76multi"), 4, "Delete all rows");

  # Test that do() reports errors from all result sets
  ok(!eval { $dbh->do("INSERT INTO dbd_mysql_t76multi VALUES (1); INSERT INTO bad_dbd_mysql_t76multi VALUES (2);") }, "do() reports errors");

  # Test that execute() reports errors from only the first result set
  ok($sth = $dbh->prepare("UPDATE dbd_mysql_t76multi SET a=2; UPDATE bad_dbd_mysql_t76multi SET a=3"));
  ok($sth->execute(), "Execute updates");
  ok(!$sth->err(), "Err was not set after execute");
  ok(!eval { $sth->more_results(); 1 });
  ok($sth->err(), "Err was set after more_results");
  ok $dbh->do("DROP TABLE dbd_mysql_t76multi");

  # Check that last_insert_id works for more_results too
  ok($dbh->do("CREATE TEMPORARY TABLE dbd_mysql_t76multi2 (a INT AUTO_INCREMENT PRIMARY KEY)"));
  ok($sth = $dbh->prepare("INSERT INTO dbd_mysql_t76multi2 VALUES(); INSERT INTO dbd_mysql_t76multi2 VALUES();"));
  ok($sth->execute());
  is($sth->last_insert_id(), 1);
  is($dbh->last_insert_id(undef, undef, undef, undef), 1);
  ok($sth->more_results());
  is($sth->last_insert_id(), 2);
  is($dbh->last_insert_id(undef, undef, undef, undef), 2);
  ok(not $sth->more_results());

  # Check that $dbh->last_insert_id works after $dbh->do with multi statements
  ok($dbh->do("INSERT INTO dbd_mysql_t76multi2 VALUES(3); INSERT INTO dbd_mysql_t76multi2 VALUES(4);"));
  is($dbh->last_insert_id(undef, undef, undef, undef), 4, '$dbh->last_insert_id is correct after $dbh->do with multi statements');

  # Check that $dbh->last_insert_id works after prepare, execute and finish but without more_results
  ok($sth = $dbh->prepare("INSERT INTO dbd_mysql_t76multi2 VALUES(5); INSERT INTO dbd_mysql_t76multi2 VALUES(6);"));
  ok($sth->execute());
  ok($sth->finish());
  is($dbh->last_insert_id(undef, undef, undef, undef), 6, '$dbh->last_insert_id is correct after multi statement prepare, execute and finish');

  # Check that $dbh->last_insert_id works after prepare and execute, but without more_results; after preparing new statement
  ok($sth = $dbh->prepare("INSERT INTO dbd_mysql_t76multi2 VALUES(7); INSERT INTO dbd_mysql_t76multi2 VALUES(8);"));
  ok($sth->execute());
  ok($sth = $dbh->prepare("SELECT 1"));
  is($dbh->last_insert_id(undef, undef, undef, undef), 8, '$dbh->last_insert_id is correct after multi statement prepare and execute without finish followed by new prepare');

  # Check that $dbh->last_insert_id works after prepare and execute, but without more_results; after calling $dbh->do which do not modify last_insert_id
  ok($sth = $dbh->prepare("INSERT INTO dbd_mysql_t76multi2 VALUES(9); INSERT INTO dbd_mysql_t76multi2 VALUES(10);"));
  ok($sth->execute());
  ok($dbh->do("SELECT 1"));
  is($dbh->last_insert_id(undef, undef, undef, undef), 10, '$dbh->last_insert_id is correct after multi statement prepare and execute without finish followed by $dbh->do');
  ok($sth->finish());

  # Check that statement attributes are correct after calling more_results
  ok($dbh->do("CREATE TEMPORARY TABLE dbd_mysql_t76multi3 (name_id1 INT, name_id2 INT)"));
  ok($sth = $dbh->prepare("SELECT name_id1 FROM dbd_mysql_t76multi3; INSERT INTO dbd_mysql_t76multi2 VALUES(11); SELECT name_id1, name_id2 FROM dbd_mysql_t76multi3; SYNTAX ERROR"));
  ok($sth->execute());
  is($sth->{NUM_OF_FIELDS}, 1);
  is($sth->{NUM_OF_PARAMS}, 0);
  is_deeply($sth->{NAME}, [ 'name_id1' ]);
  is_deeply($sth->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);
  ok($sth->more_results());
  is($sth->{NUM_OF_FIELDS}, 0);
  is($sth->{NUM_OF_PARAMS}, 0);
  ok(!eval { $sth->{NAME} });
  ok(!eval { $sth->{mariadb_type} });
  ok($sth->more_results());
  is($sth->{NUM_OF_FIELDS}, 2);
  is($sth->{NUM_OF_PARAMS}, 0);
  is_deeply($sth->{NAME}, [ 'name_id1', 'name_id2' ]);
  is_deeply($sth->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG, DBD::MariaDB::TYPE_LONG ]);
  ok(!eval { $sth->more_results() });
  is($sth->{NUM_OF_FIELDS}, 0);
  is($sth->{NUM_OF_PARAMS}, 0);
  ok(!eval { $sth->{NAME} });
  ok(!eval { $sth->{mariadb_type} });

$dbh->disconnect();
