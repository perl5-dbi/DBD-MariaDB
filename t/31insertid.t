use strict;
use warnings;

use DBI;
use Test::More;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                         { RaiseError => 1, PrintError => 0 });

plan tests => 3 + 2*35;

SKIP: {
    skip 'SET @@auto_increment_offset needs MySQL >= 5.0.2', 2 unless $dbh->{mariadb_serverversion} >= 50002;
    ok $dbh->do('SET @@auto_increment_offset = 1');
    ok $dbh->do('SET @@auto_increment_increment = 1');
}

for my $mariadb_server_prepare (0, 1) {

$dbh->{mariadb_server_prepare} = $mariadb_server_prepare;

my $create = <<EOT;
CREATE TEMPORARY TABLE dbd_mysql_t31 (
  id INT(3) PRIMARY KEY AUTO_INCREMENT NOT NULL,
  name VARCHAR(64))
EOT

ok $dbh->do($create), "create dbd_mysql_t31";

my $query= "INSERT INTO dbd_mysql_t31 (name) VALUES (?)";

my $sth;
ok ($sth= $dbh->prepare($query));

ok defined $sth;

ok $sth->execute("Jochen");

is $sth->{mariadb_insertid}, 1, "insert id == $sth->{mariadb_insertid}";
is $dbh->{mariadb_insertid}, 1, "insert id == $dbh->{mariadb_insertid}";
is $sth->last_insert_id(), 1, "insert id == \$sth->last_insert_id()";
is $dbh->last_insert_id(undef, undef, undef, undef), 1, "insert id == \$dbh->last_insert_id()";

ok $sth->execute("Patrick");

$dbh->ping();
is $sth->{mariadb_insertid}, 2, "insert id == $sth->{mariadb_insertid}";
is $dbh->{mariadb_insertid}, 2, "insert id == $dbh->{mariadb_insertid}";
is $sth->last_insert_id(), 2, "insert id == \$sth->last_insert_id()";
is $dbh->last_insert_id(undef, undef, undef, undef), 2, "insert id == \$dbh->last_insert_id()";

ok (my $sth2= $dbh->prepare("SELECT max(id) FROM dbd_mysql_t31"));

ok defined $sth2;

ok $sth2->execute();

my $max_id;
ok ($max_id= $sth2->fetch());

ok defined $max_id;

  cmp_ok $dbh->{mariadb_insertid}, '==', $max_id->[0],
    "dbh insert id $dbh->{'mariadb_insertid'} == max(id) $max_id->[0] in dbd_mysql_t31";
cmp_ok $sth->{mariadb_insertid}, '==', $max_id->[0],
  "sth insert id $sth->{'mariadb_insertid'} == max(id) $max_id->[0]  in dbd_mysql_t31";

ok (my $sth3 = $dbh->prepare("INSERT INTO dbd_mysql_t31 (name) VALUES (?)"));

ok $sth3->execute("Name");

is $sth->{mariadb_insertid}, 2, "second insert id == $sth->{mariadb_insertid}";
is $sth->last_insert_id(), 2, "second insert id == \$sth->last_insert_id()";
is $dbh->{mariadb_insertid}, 3, "third insert id == $dbh->{mariadb_insertid}";
is $sth3->last_insert_id(), 3, "third insert id == \$sth3->last_insert_id()";
is $dbh->last_insert_id(undef, undef, undef, undef), 3, "third insert id == \$dbh->last_insert_id()";

ok $dbh->do($query, undef, "Name 2"), "inserting fourth value via \$dbh->do()";
is $dbh->{mariadb_insertid}, 4, "fourth insert id == $dbh->{mariadb_insertid}";
is $dbh->last_insert_id(undef, undef, undef, undef), 4, "fourth insert id == \$dbh->last_insert_id()";
is $sth->{mariadb_insertid}, 2, "second insert id == \$sth->{mariadb_insertid}";
is $sth->last_insert_id(), 2, "second insert id == \$sth->last_insert_id()";
is $sth3->{mariadb_insertid}, 3, "third insert id == \$sth3->{mariadb_insertid}";
is $sth3->last_insert_id(), 3, "third insert id == \$sth3->last_insert_id()";

ok $dbh->do('DROP TEMPORARY TABLE dbd_mysql_t31');

}

ok $dbh->disconnect();
