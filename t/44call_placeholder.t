use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, mariadb_server_prepare => 1 });

plan skip_all => "CREATE PROCEDURE is supported since MySQL version 5.0" if $dbh->{mariadb_serverversion} < 50000;

plan skip_all => $dbh->errstr() if !CheckRoutinePerms($dbh);

plan tests => 12;

ok $dbh->do('DROP PROCEDURE IF EXISTS t44_call_placeholder');

ok $dbh->do(<<EOPROC);
CREATE PROCEDURE t44_call_placeholder(IN param INT)
BEGIN
  SELECT 5, param;
END
EOPROC

ok my $sth = $dbh->prepare('CALL t44_call_placeholder(?)');
ok $sth->execute(10);
is_deeply $sth->fetchall_arrayref(), [ [5, 10] ];

ok $dbh->do('DROP PROCEDURE t44_call_placeholder');

ok $dbh->do('CREATE TEMPORARY TABLE t(a INT, b INT)');

ok $dbh->do(<<EOPROC);
CREATE PROCEDURE t44_call_placeholder(IN param INT)
BEGIN
  INSERT INTO t(a, b) SELECT 5, param;
END
EOPROC

ok $dbh->do('CALL t44_call_placeholder(?)', undef, 10);
is_deeply $dbh->selectall_arrayref('SELECT a, b FROM t'), [ [5, 10] ];

ok $dbh->do('DROP PROCEDURE t44_call_placeholder');

ok $dbh->disconnect();
