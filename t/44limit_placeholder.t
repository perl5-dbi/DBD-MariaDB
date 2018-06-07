use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 1, AutoCommit => 0, mariadb_server_prepare => 1 });

plan tests => 16;

ok $dbh->do('CREATE TEMPORARY TABLE t(id INT)');
ok $dbh->do('INSERT INTO t(id) VALUES(10)');
ok $dbh->do('INSERT INTO t(id) VALUES(20)');

ok my $sth = $dbh->prepare('SELECT id FROM t ORDER BY id LIMIT ?');
ok $sth->execute(1);
is_deeply $sth->fetchall_arrayref(), [ [10] ];
ok $sth->finish();

ok $sth = $dbh->prepare('SELECT id FROM t ORDER BY id LIMIT ?,?');
ok $sth->execute(0, 1);
is_deeply $sth->fetchall_arrayref(), [ [10] ];
ok $sth->finish();

ok $sth = $dbh->prepare('SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?');
ok $sth->execute(1, 0);
is_deeply $sth->fetchall_arrayref(), [ [10] ];
ok $sth->finish();

ok $dbh->disconnect();
