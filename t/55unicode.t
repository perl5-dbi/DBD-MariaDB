use strict;
use warnings;
use utf8;

use Test::More;
use DBI;

use vars qw($test_user $test_password $test_dsn);
use lib 't', '.';
require 'lib.pl';

my $tb = Test::More->builder;
binmode $tb->output,         ":utf8";
binmode $tb->failure_output, ":utf8";
binmode $tb->todo_output,    ":utf8";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

my $utf8mb4 = $dbh->selectrow_array("SHOW CHARSET LIKE 'utf8mb4'") ? 1 : 0;

plan tests => 11*2*2;

for my $mariadb_server_prepare (0, 1) {
    $dbh->{mariadb_server_prepare} = $mariadb_server_prepare;
    for my $ins ("\xC3", "\N{U+C3}", "\xC3\xA1", "\N{U+C3}\N{U+A1}", "á", "č", "\x{263A}", "\N{U+263A}", "☺", "\N{U+1F4A9}", "💩") {
        SKIP: {
            skip 'Server does not support utf8mb4', 1 if $ins =~ /[^\x{0000}-\x{FFFF}]/ and not $utf8mb4;
            $dbh->do("CREATE TEMPORARY TABLE t(s VARCHAR(10))");
            my $sth = $dbh->prepare("INSERT INTO t(s) VALUES('$ins')");
            $sth->execute();
            my $fetch = $dbh->selectrow_array("SELECT s FROM t");
            is($fetch, $ins, "test $ins without bind");
            $dbh->do("DROP TEMPORARY TABLE t");
        }
        SKIP: {
            skip 'Server does not support utf8mb4', 1 if $ins =~ /[^\x{0000}-\x{FFFF}]/ and not $utf8mb4;
            $dbh->do("CREATE TEMPORARY TABLE t(s VARCHAR(10))");
            my $sth = $dbh->prepare("INSERT INTO t(s) VALUES(?)");
            $sth->execute($ins);
            my $fetch = $dbh->selectrow_array("SELECT s FROM t");
            is($fetch, $ins, "test $ins with bind");
            $dbh->do("DROP TEMPORARY TABLE t");
        }
    }
}

$dbh->disconnect();
