use strict;
use warnings;

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my ($dbh, $sth, $aref);
$dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });
plan tests => 2*36 + 1;

for my $mariadb_server_prepare (0, 1) {

$dbh->{mariadb_server_prepare} = $mariadb_server_prepare;

my $create= <<EOT;
CREATE TEMPORARY TABLE dbd_mysql_t40numrows (
  id INT(4) NOT NULL DEFAULT 0,
  name varchar(64) NOT NULL DEFAULT ''
)
EOT

ok $dbh->do($create), "create table dbd_mysql_t40numrows";

ok $dbh->do("INSERT INTO dbd_mysql_t40numrows VALUES( 1, 'Alligator Descartes' )"), 'inserting first row';

ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40numrows WHERE id = 1"));

is $sth->rows, -1, '$sth->rows prior execute is unknown (-1)';

ok $sth->execute;

is $sth->rows, 1, '$sth->rows should be 1';

ok ($aref= $sth->fetchall_arrayref);

is scalar @$aref, 1, 'Verified rows should be 1';

is $sth->rows, 1, '$sth->rows still should be 1';

ok $dbh->do("INSERT INTO dbd_mysql_t40numrows VALUES( 2, 'Jochen Wiedmann' )"), 'inserting second row';

ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40numrows WHERE id >= 1"));

is $sth->rows, -1, '$sth->rows prior execute is unknown (-1)';

ok $sth->execute;

is $sth->rows, 2, '$sth->rows should be 2';

ok ($aref= $sth->fetchall_arrayref);

is scalar @$aref, 2, 'Verified rows should be 2';

is $sth->rows, 2, '$sth->rows still should be 2';

ok $dbh->do("INSERT INTO dbd_mysql_t40numrows VALUES(3, 'Tim Bunce')"), "inserting third row";

ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40numrows WHERE id >= 2"));

is $sth->rows, -1, '$sth->rows prior execute is unknown (-1)';

ok $sth->execute;

is $sth->rows, 2, 'rows should be 2';

ok ($aref= $sth->fetchall_arrayref);

is scalar @$aref, 2, 'Verified rows should be 2';

is $sth->rows, 2, 'rows still should be 2';

ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40numrows"));

is $sth->rows, -1, '$sth->rows prior execute is unknown (-1)';

ok $sth->execute;

is $sth->rows, 3, 'rows should be 3';

ok ($aref= $sth->fetchall_arrayref);

is scalar @$aref, 3, 'Verified rows should be 3';

is $sth->rows, 3, 'rows still should be 3';

my $sth = eval { $dbh->prepare("SYNTAX ERROR") };
if ($sth) {
  is $sth->rows, -1, '$sth->rows prior execute is unknown (-1)';
  ok !eval { $sth->execute() }, '$sth->execute for SYNTAX ERROR failed';
  is $sth->rows, -1, '$sth->rows for SYNTAX ERROR is unknown (-1)';
} else {
  pass '$dbh->prepare for SYNTAX ERROR failed' for 1..3;
}

ok $dbh->do("DROP TEMPORARY TABLE dbd_mysql_t40numrows"), "drop table dbd_mysql_t40numrows";

}

ok $dbh->disconnect;
