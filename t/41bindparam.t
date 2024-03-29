use strict;
use warnings;

use DBI;
use Test::More;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my ($dbh, $sth);
$dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });
plan tests => 10;

ok $dbh->do("drop table if exists dbd_mysql_41bindparam"), "drop table dbd_mysql_41bindparam";

ok $dbh->do("create table dbd_mysql_41bindparam (a int not null, primary key (a))"), "create table dbd_mysql_41bindparam";

ok ($sth= $dbh->prepare("insert into dbd_mysql_41bindparam values (?)"));

ok $sth->bind_param(1,10000,DBI::SQL_INTEGER), "bind param 10000 col1";

ok $sth->execute(), 'execute';

ok $sth->bind_param(1,10001,DBI::SQL_INTEGER), "bind param 10001 col1";

ok $sth->execute(), 'execute';

ok ($sth= $dbh->prepare("DROP TABLE dbd_mysql_41bindparam"));

ok $sth->execute();

ok $dbh->disconnect;
