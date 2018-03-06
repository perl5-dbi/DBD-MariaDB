use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });

plan tests => 11;

ok($dbh->do("CREATE TEMPORARY TABLE t(id INT)"));
ok($dbh->do("INSERT INTO t(id) VALUES(1)"));

my $sth = $dbh->prepare("SELECT * FROM t");
ok($sth->execute());
ok($sth->fetchall_arrayref());
is_deeply($sth->{NAME}, ["id"]);
ok($sth->finish());

my $sth2 = $dbh->prepare("SELECT * FROM t", { mariadb_server_prepare => 1 });
ok($sth2->execute());
ok($sth2->fetchall_arrayref());
is_deeply($sth2->{NAME}, ["id"]);
ok($sth2->finish());

ok($dbh->disconnect());
