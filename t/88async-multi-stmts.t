use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 0, PrintError => 0, AutoCommit => 0, mariadb_multi_statements => 1 });
plan tests => 104;

ok $dbh->do(<<SQL);
CREATE TEMPORARY TABLE async_test (
    value INTEGER AUTO_INCREMENT PRIMARY KEY
);
SQL

my $sth0 = $dbh->prepare('INSERT INTO async_test VALUES(1)', { mariadb_async => 1 });
ok $sth0 or die $dbh->err;
my $sth1 = $dbh->prepare('INSERT INTO async_test VALUES(2)', { mariadb_async => 1 });
ok $sth1 or die $dbh->err;
my $sth2 = $dbh->prepare('INSERT INTO async_test VALUES(3); INSERT INTO async_test VALUES(4);', { mariadb_async => 1 });
ok $sth2 or die $dbh->err;

ok !$sth0->{Active};
ok !$sth1->{Active};
ok !$sth2->{Active};

ok $sth0->execute;

ok $sth0->{Active};
ok !$sth1->{Active};
ok !$sth2->{Active};

ok !defined($sth1->mariadb_async_ready);
ok $sth1->errstr;
ok !defined($sth1->mariadb_async_result);
ok $sth1->errstr;

ok defined($sth0->mariadb_async_ready);
ok !$sth1->errstr;
ok defined($sth0->mariadb_async_result);
ok !$sth1->errstr;

ok !$sth0->{Active};
ok !$sth1->{Active};
ok !$sth2->{Active};

is($sth0->last_insert_id(), 1);
is($dbh->last_insert_id(undef, undef, undef, undef), 1);

ok $sth2->execute;

ok !$sth0->{Active};
ok !$sth1->{Active};
ok $sth2->{Active};

ok !defined($sth1->mariadb_async_ready);
ok $sth1->err;
ok !defined($sth1->mariadb_async_result);
ok $sth1->err;

ok !$sth0->{Active};
ok !$sth1->{Active};
ok $sth2->{Active};

is($sth0->last_insert_id(), 1);
is($dbh->last_insert_id(undef, undef, undef, undef), 1);

ok defined($sth2->mariadb_async_ready);
ok !$sth2->err;

is($sth0->last_insert_id(), 1);
is($dbh->last_insert_id(undef, undef, undef, undef), 1);

ok defined($sth2->mariadb_async_result);
ok !$sth2->err;

ok !$sth0->{Active};
ok !$sth1->{Active};
ok $sth2->{Active};

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 3);
is($dbh->last_insert_id(undef, undef, undef, undef), 3);

ok $sth2->more_results;
ok defined($sth2->mariadb_async_result);
ok !$sth2->err;

ok !$sth0->{Active};
ok !$sth1->{Active};
ok !$sth2->{Active};

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 4);
is($dbh->last_insert_id(undef, undef, undef, undef), 4);

ok !$sth2->more_results;

ok !$sth0->{Active};
ok !$sth1->{Active};
ok !$sth2->{Active};

ok $dbh->do('INSERT INTO async_test VALUES(5)', { mariadb_async => 1 });

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

my $sth3 = $dbh->prepare('INSERT INTO async_test VALUES(6); INSERT INTO async_test VALUES(7); SYNTAX ERROR; INSERT INTO async_test VALUES(8)', { mariadb_async => 1 });

ok $sth3 or die $dbh->err;

ok !$sth3->{Active};
ok $sth3->execute();
ok $sth3->{Active};

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 4);
is($dbh->last_insert_id(undef, undef, undef, undef), 5);

ok $sth3->{Active};
ok $sth3->mariadb_async_result();
ok $sth3->{Active};

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 4);
is($sth3->last_insert_id(), 6);
is($dbh->last_insert_id(undef, undef, undef, undef), 6);

ok $sth3->{Active};
ok $sth3->more_results();
ok $sth3->{Active};

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 4);
is($sth3->last_insert_id(), 7);
is($dbh->last_insert_id(undef, undef, undef, undef), 7);

ok $sth3->{Active};
ok !$sth3->more_results();
ok !$sth3->{Active};

is($sth0->last_insert_id(), 1);
is($sth2->last_insert_id(), 4);
is($sth3->last_insert_id(), 7);
is($dbh->last_insert_id(undef, undef, undef, undef), 7);

local $SIG{__WARN__} = sub { die @_ };

$dbh->disconnect;
