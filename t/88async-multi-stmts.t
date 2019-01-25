use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 0, PrintError => 0, AutoCommit => 0, mariadb_multi_statements => 1 });
plan tests => 45;

$dbh->do(<<SQL);
CREATE TEMPORARY TABLE async_test (
    value INTEGER AUTO_INCREMENT PRIMARY KEY
);
SQL

my $sth0 = $dbh->prepare('INSERT INTO async_test VALUES(1)', { mariadb_async => 1 });
my $sth1 = $dbh->prepare('INSERT INTO async_test VALUES(2)', { mariadb_async => 1 });
my $sth2 = $dbh->prepare('INSERT INTO async_test VALUES(3); INSERT INTO async_test VALUES(4);', { mariadb_async => 1 });

$sth0->execute;
ok !defined($sth1->mariadb_async_ready);
ok $sth1->errstr;
ok !defined($sth1->mariadb_async_result);
ok $sth1->errstr;

ok defined($sth0->mariadb_async_ready);
ok !$sth1->errstr;
ok defined($sth0->mariadb_async_result);
ok !$sth1->errstr;

is($sth0->last_insert_id(), 1);
is($dbh->last_insert_id(undef, undef, undef, undef), 1);

$sth2->execute;
ok !defined($sth1->mariadb_async_ready);
ok $sth1->err;
ok !defined($sth1->mariadb_async_result);
ok $sth1->err;

is($sth0->last_insert_id(), 1);
is($dbh->last_insert_id(undef, undef, undef, undef), 1);

ok defined($sth2->mariadb_async_ready);
ok !$sth2->err;

is($sth0->last_insert_id(), 1);
is($dbh->last_insert_id(undef, undef, undef, undef), 1);

ok defined($sth2->mariadb_async_result);
ok !$sth2->err;

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 3);
is($dbh->last_insert_id(undef, undef, undef, undef), 3);

ok $sth2->more_results;
ok defined($sth2->mariadb_async_result);
ok !$sth2->err;

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 4);
is($dbh->last_insert_id(undef, undef, undef, undef), 4);

ok !$sth2->more_results;

$dbh->do('INSERT INTO async_test VALUES(5)', { mariadb_async => 1 });

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 4);
is($dbh->last_insert_id(undef, undef, undef, undef), 4);

ok defined($dbh->mariadb_async_ready);
ok !$dbh->err;

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 4);
is($dbh->last_insert_id(undef, undef, undef, undef), 4);

ok defined($dbh->mariadb_async_result);
ok !$sth2->err;

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 4);
is($dbh->last_insert_id(undef, undef, undef, undef), 5);

undef $sth0;
undef $sth1;
undef $sth2;

$dbh->disconnect;
