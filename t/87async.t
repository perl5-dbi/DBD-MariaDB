use strict;
use warnings;

use Test::Deep;
use Test::More;
use DBI;
use DBI::Const::GetInfoType;
use Time::HiRes;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 0, PrintError => 0, AutoCommit => 0 });
if ($dbh->{mariadb_serverversion} < 50012) {
    plan skip_all => "Servers < 5.0.12 do not support SLEEP()";
}
if ($dbh->{mariadb_hostinfo} eq 'Embedded') {
    plan skip_all => 'Async mode is not supported for Embedded server';
}
plan tests => 150;

is $dbh->get_info($GetInfoType{'SQL_ASYNC_MODE'}), 2; # statement-level async
is $dbh->get_info($GetInfoType{'SQL_MAX_ASYNC_CONCURRENT_STATEMENTS'}), 1;

$dbh->do(<<SQL);
CREATE TEMPORARY TABLE async_test (
    value0 INTEGER,
    value1 INTEGER,
    value2 INTEGER
);
SQL

cmp_ok $dbh->mariadb_sockfd, '>=', 0;
ok !defined($dbh->mariadb_async_ready);

my ( $start, $end );
my $rows;
my $sth;
my ( $a, $b, $c );

$start = Time::HiRes::gettimeofday();
$rows = $dbh->do('INSERT INTO async_test VALUES (SLEEP(2), 0, 0)');
$end = Time::HiRes::gettimeofday();

is $rows, 1;
cmp_ok(($end - $start), '>=', 1.9);

$start = Time::HiRes::gettimeofday();
$rows = $dbh->do('INSERT INTO async_test VALUES (SLEEP(2), 0, 0)', { mariadb_async => 1 });
ok defined($dbh->mariadb_async_ready);
$end = Time::HiRes::gettimeofday();

ok $rows;
is $rows, '0E0';

cmp_ok(($end - $start), '<', 2);

for (1..120) {
  last if $dbh->mariadb_async_ready;
  sleep 1;
}
ok $dbh->mariadb_async_ready;
$end = Time::HiRes::gettimeofday();
cmp_ok(($end - $start), '>=', 1.9);

$rows = $dbh->mariadb_async_result;
ok !defined($dbh->mariadb_async_ready);

is $rows, 1;

$start = Time::HiRes::gettimeofday();
$rows = $dbh->do('INSERT INTO async_test VALUES (SLEEP(2), 0, 0)', { mariadb_async => 1 });
ok defined($dbh->mariadb_async_ready);
$end = Time::HiRes::gettimeofday();

ok $rows;
is $rows, '0E0';

cmp_ok(($end - $start), '<', 2);

my $fd = $dbh->mariadb_sockfd;
my $bits = '';
vec($bits, $fd, 1) = 1;
my $nfound = select(my $rout = $bits, undef, my $eout = $bits, 120);
is $nfound, 1;
ok vec($rout, $fd, 1);
ok !vec($eout, $fd, 1);

$end = Time::HiRes::gettimeofday();
cmp_ok(($end - $start), '>=', 1.9);

$rows = $dbh->mariadb_async_result;
ok !defined($dbh->mariadb_async_ready);

is $rows, 1;

( $rows ) = $dbh->selectrow_array('SELECT COUNT(1) FROM async_test');

is $rows, 3;

$dbh->do('DELETE FROM async_test');

$start = Time::HiRes::gettimeofday();
$rows = $dbh->do('INSERT INTO async_test VALUES(SLEEP(2), ?, ?)', { mariadb_async => 1 }, 1, 2);
$end = Time::HiRes::gettimeofday();

ok $rows;
is $rows, '0E0';

cmp_ok(($end - $start), '<', 2);

for (1..120) {
  last if $dbh->mariadb_async_ready;
  sleep 1;
}
ok $dbh->mariadb_async_ready;
$end = Time::HiRes::gettimeofday();
cmp_ok(($end - $start), '>=', 1.9);

$rows = $dbh->mariadb_async_result;

is $rows, 1;

( $a, $b, $c ) = $dbh->selectrow_array('SELECT * FROM async_test');

is $a, 0;
is $b, 1;
is $c, 2;

$sth = $dbh->prepare('SELECT SLEEP(2)');
ok !$sth->{Active};
ok !defined($sth->mariadb_async_ready);
$start = Time::HiRes::gettimeofday();
ok $sth->execute;
$end = Time::HiRes::gettimeofday();
cmp_ok(($end - $start), '>=', 1.9);
ok $sth->{Active};
ok $sth->finish;
ok !$sth->{Active};

$sth = $dbh->prepare('SELECT SLEEP(2)', { mariadb_async => 1 });
ok !$sth->{Active};
ok !defined($sth->mariadb_async_ready);
$start = Time::HiRes::gettimeofday();
ok $sth->execute;
ok defined($sth->mariadb_async_ready);
$end = Time::HiRes::gettimeofday();
cmp_ok(($end - $start), '<', 2);
ok $sth->{Active};

for (1..120) {
  last if $sth->mariadb_async_ready;
  sleep 1;
}
ok $sth->mariadb_async_ready;

ok $sth->{Active};
my $row = $sth->fetch;
ok !$sth->{Active};
$end = Time::HiRes::gettimeofday();
ok $row;
is $row->[0], 0;
cmp_ok(($end - $start), '>=', 1.9);

$rows = $dbh->do('INSERT INTO async_test VALUES(SLEEP(2), ?, ?', { mariadb_async => 1 }, 1, 2);

ok $rows;
ok !$dbh->err;
$rows = $dbh->mariadb_async_result;
ok !$rows;
ok $dbh->err;

$dbh->do('DELETE FROM async_test');

$sth = $dbh->prepare('INSERT INTO async_test VALUES(SLEEP(2), ?, ?)', { mariadb_async => 1 });
ok !$sth->{Active};
$start = Time::HiRes::gettimeofday();
$rows = $sth->execute(1, 2);
$end = Time::HiRes::gettimeofday();
cmp_ok(($end - $start), '<', 2);
ok $sth->{Active};
ok $rows;
is $rows, '0E0';

$rows = $sth->mariadb_async_result;
$end = Time::HiRes::gettimeofday();
cmp_ok(($end - $start), '>=', 1.9);
ok !$sth->{Active};
is $rows, 1;

( $a, $b, $c ) = $dbh->selectrow_array('SELECT * FROM async_test');

is $a, 0;
is $b, 1;
is $c, 2;

$sth  = $dbh->prepare('INSERT INTO async_test VALUES(SLEEP(2), ?, ?)', { mariadb_async => 1 });
$rows = $dbh->do('INSERT INTO async_test VALUES(SLEEP(2), ?, ?)', undef, 1, 2);
is $rows, 1;

$start = Time::HiRes::gettimeofday();
$dbh->selectrow_array('SELECT SLEEP(2)', { mariadb_async => 1 });
$end = Time::HiRes::gettimeofday();

cmp_ok(($end - $start), '>=', 1.9);
ok !defined($dbh->mariadb_async_result);
ok !defined($dbh->mariadb_async_ready);

$rows = $dbh->do('UPDATE async_test SET value0 = 0 WHERE value0 = 999', { mariadb_async => 1 });
ok $rows;
is $rows, '0E0';
$rows = $dbh->mariadb_async_result;
ok $rows;
is $rows, '0E0';

$sth  = $dbh->prepare('UPDATE async_test SET value0 = 0 WHERE value0 = 999', { mariadb_async => 1 });
ok !$sth->{Active};
$rows = $sth->execute;
ok $sth->{Active};
ok $rows;
is $rows, '0E0';
$rows = $sth->mariadb_async_result;
ok !$sth->{Active};
ok $rows;
is $rows, '0E0';

$sth->execute;
$rows = $dbh->do('INSERT INTO async_test VALUES(1, 2, 3)');
ok !$rows;
undef $sth;
$rows = $dbh->do('INSERT INTO async_test VALUES(1, 2, 3)');
is $rows, 1;

$sth = $dbh->prepare('SELECT 1, value0, value1, value2 FROM async_test WHERE value0 = ?', { mariadb_async => 1 });
ok !$sth->{Active};
$sth->execute(1);
ok $sth->{Active};
is $sth->{'NUM_OF_FIELDS'}, undef;
is $sth->{'NUM_OF_PARAMS'}, 1;
is $sth->{'NAME'}, undef;
is $sth->{'NAME_lc'}, undef;
is $sth->{'NAME_uc'}, undef;
is $sth->{'NAME_hash'}, undef;
is $sth->{'NAME_lc_hash'}, undef;
is $sth->{'NAME_uc_hash'}, undef;
is $sth->{'TYPE'}, undef;
is $sth->{'PRECISION'}, undef;
is $sth->{'SCALE'}, undef;
is $sth->{'NULLABLE'}, undef;
is $sth->{'Database'}, $dbh;
is $sth->{'Statement'}, 'SELECT 1, value0, value1, value2 FROM async_test WHERE value0 = ?';
$sth->mariadb_async_result;
ok $sth->{Active};
is $sth->{'NUM_OF_FIELDS'}, 4;
is $sth->{'NUM_OF_PARAMS'}, 1;
cmp_bag $sth->{'NAME'}, [qw/1 value0 value1 value2/];
cmp_bag $sth->{'NAME_lc'}, [qw/1 value0 value1 value2/];
cmp_bag $sth->{'NAME_uc'}, [qw/1 VALUE0 VALUE1 VALUE2/];
cmp_bag [ keys %{$sth->{'NAME_hash'}} ], [qw/1 value0 value1 value2/];
cmp_bag [ keys %{$sth->{'NAME_lc_hash'}} ], [qw/1 value0 value1 value2/];
cmp_bag [ keys %{$sth->{'NAME_uc_hash'}} ], [qw/1 VALUE0 VALUE1 VALUE2/];
is ref($sth->{'TYPE'}), 'ARRAY';
is ref($sth->{'PRECISION'}), 'ARRAY';
is ref($sth->{'SCALE'}), 'ARRAY';
is ref($sth->{'NULLABLE'}), 'ARRAY';
is $sth->{'Database'}, $dbh;
is $sth->{'Statement'}, 'SELECT 1, value0, value1, value2 FROM async_test WHERE value0 = ?';
$sth->finish;
ok !$sth->{Active};

$sth->execute(1);
ok $sth->{Active};
$row = $sth->fetch;
ok !$sth->{Active};
is_deeply $row, [1, 1, 2, 3];
is $sth->rows, 1;

$sth->execute(1);
ok $sth->{Active};
$sth->mariadb_async_result;
ok $sth->{Active};
$row = $sth->fetch;
ok !$sth->{Active};
is_deeply $row, [1, 1, 2, 3];
is $sth->rows, 1;

$sth->execute(1);
ok $sth->{Active};
$row = $sth->fetchrow_arrayref;
ok !$sth->{Active};
is_deeply $row, [1, 1, 2, 3];
is $sth->rows, 1;

$sth->execute(1);
ok $sth->{Active};
my @row = $sth->fetchrow_array;
ok !$sth->{Active};
is_deeply \@row, [1, 1, 2, 3];
is $sth->rows, 1;

$sth->execute(1);
ok $sth->{Active};
$row = $sth->fetchrow_hashref;
ok !$sth->{Active};
cmp_bag [ keys %$row ], [qw/1 value0 value1 value2/];
cmp_bag [ values %$row ], [1, 1, 2, 3];
is $sth->rows, 1;

$sth = $dbh->prepare('UPDATE async_test SET value0 = 2 WHERE value0 = 1', { mariadb_async => 1 });
ok !$sth->{Active};
ok $sth->execute();
ok $sth->{Active};
ok $sth->mariadb_async_result;
ok !$sth->{Active};

$sth = $dbh->prepare('SYNTAX ERROR', { mariadb_async => 1 });
ok !$sth->{Active};
ok $sth->execute();
ok $sth->{Active};
ok !$sth->mariadb_async_result;
ok !$sth->{Active};

local $SIG{__WARN__} = sub { die @_ };

ok $dbh->disconnect;
