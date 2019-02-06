use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $id1 = 'session:06b6d2138df949524092eefc066ee5ab3598bf96';
my $id2 = q(string\\string"string'string);
my $id2_quoted_no_backslash = q('string\\string"string''string');

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

if ($dbh->{mariadb_serverversion} < 50001) {
    plan skip_all => "Servers < 5.0.1 do not support sql_mode NO_BACKSLASH_ESCAPES";
}

if ($dbh->{mariadb_clientversion} < 50001) {
    $id2_quoted_no_backslash = q(X'737472696E675C737472696E6722737472696E6727737472696E67');
}

plan tests => 20;

ok $dbh->do('CREATE TEMPORARY TABLE t(id VARCHAR(255), value TEXT)');
ok $dbh->do('INSERT INTO t(id, value) VALUES(?, ?)', undef, $id1, 'value1');
ok $dbh->do('INSERT INTO t(id, value) VALUES(?, ?)', undef, $id2, 'value2');

ok $dbh->do("SET sql_mode = 'NO_BACKSLASH_ESCAPES'");

is $dbh->quote($id2), $id2_quoted_no_backslash;

for my $server_prepare (0, 1) {

    $dbh->{mariadb_server_prepare} = $server_prepare;
    $dbh->{mariadb_server_prepare_disable_fallback} = 1;

    ok my $sth = $dbh->prepare('SELECT * FROM t WHERE id = ?');
    ok $sth->bind_param(1, $id1);
    ok $sth->execute();
    is_deeply $sth->fetchall_arrayref(), [ [ $id1, 'value1' ] ];

    ok $sth->bind_param(1, $id2);
    ok $sth->execute();
    is_deeply $sth->fetchall_arrayref(), [ [ $id2, 'value2' ] ];

}

ok $dbh->disconnect();
