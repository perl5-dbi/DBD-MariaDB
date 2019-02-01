use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use vars qw($COL_NULLABLE $COL_KEY);
use lib 't', '.';
require 'lib.pl';

$test_dsn .= ';mariadb_server_prepare=1;mariadb_server_prepare_disable_fallback=1';
my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

plan tests => 9;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t99_prepare");

my $create =<<EOT;
CREATE TABLE dbd_mysql_t99_prepare (
    data LONGBLOB
)
EOT

ok $dbh->do($create);

$dbh->do("insert into dbd_mysql_t99_prepare (data) values(null)");

my $sth = $dbh->prepare("select data from dbd_mysql_t99_prepare");
ok $sth->execute;
my $row = $sth->fetch;
is $row->[0] => undef;

$dbh->do("insert into dbd_mysql_t99_prepare (data) values('a')");
$sth = $dbh->prepare("select data from dbd_mysql_t99_prepare");
ok $sth->execute;
$row = $sth->fetch;
is $row->[0] => undef;
$row = $sth->fetch;
is $row->[0] => 'a';

ok $dbh->do("DROP TABLE dbd_mysql_t99_prepare");

ok $dbh->disconnect;
