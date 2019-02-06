use strict;
use warnings;

use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });
plan tests => 32;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t51bind_type_guessing"), "drop table if exists dbd_mysql_t51bind_type_guessing";

my $create= <<"EOTABLE";
create table dbd_mysql_t51bind_type_guessing (
    id bigint unsigned not null default 0
    )
EOTABLE


ok $dbh->do($create), "creating table";

my $statement= "insert into dbd_mysql_t51bind_type_guessing (id) values (?)";

my $sth1;
ok $sth1= $dbh->prepare($statement);

my $rows;
ok $rows= $sth1->execute('9999999999999999');
cmp_ok $rows, '==',  1;

$statement= "update dbd_mysql_t51bind_type_guessing set id = ?";
my $sth2;
ok $sth2= $dbh->prepare($statement);

ok $rows= $sth2->execute('9999999999999998');
cmp_ok $rows, '==',  1;

$dbh->{mariadb_bind_type_guessing}= 1;
ok $rows= $sth1->execute('9999999999999997');
cmp_ok $rows, '==',  1;

$statement= "update dbd_mysql_t51bind_type_guessing set id = ? where id = ?";

ok $sth2= $dbh->prepare($statement);
ok $rows= $sth2->execute('9999999999999996', '9999999999999997');

my $retref;
ok $retref= $dbh->selectall_arrayref("select * from dbd_mysql_t51bind_type_guessing");

cmp_ok $retref->[0][0], '==', 9999999999999998;
cmp_ok $retref->[1][0], '==', 9999999999999996;

# checking varchars/empty strings/misidentification:
$create= <<"EOTABLE";
create table dbd_mysql_t51bind_type_guessing (
    str varchar(80),
    num bigint
    )
EOTABLE
ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t51bind_type_guessing"), "drop table if exists dbd_mysql_t51bind_type_guessing";
ok $dbh->do($create), "creating table w/ varchar";
my $sth3;
ok $sth3= $dbh->prepare("insert into dbd_mysql_t51bind_type_guessing (str, num) values (?, ?)");
ok $rows= $sth3->execute(52.3, 44);
ok $rows= $sth3->execute('', '     77');
ok $rows= $sth3->execute(undef, undef);
ok $rows= $sth3->execute('.', 100);
ok $rows= $sth3->execute('+', 101);
ok $rows= $sth3->execute('-', 102);
ok $rows= $sth3->execute('+10e+100', 103);
ok $rows= $sth3->execute('-12.E-100', 104);

ok $sth3= $dbh->prepare("select * from dbd_mysql_t51bind_type_guessing limit ?");
ok $rows= $sth3->execute(1);
$sth3->finish();
ok $rows= $sth3->execute('   1');
$sth3->finish();

my $ref = $dbh->selectall_arrayref(q(SELECT * FROM dbd_mysql_t51bind_type_guessing WHERE str IN ('', '.', '+', '-')));
is_deeply($ref, [ [ '', 77 ], [ '.', 100 ], [ '+', 101 ], [ '-', 102 ] ] );

ok $dbh->do("DROP TABLE dbd_mysql_t51bind_type_guessing");
ok $dbh->disconnect;
