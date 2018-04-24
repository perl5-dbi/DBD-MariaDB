use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });

plan tests => 31;

ok eval { $dbh->do('SELECT 1', {}) }, 'empty set of attributes is accepted for ->do method';
ok !eval { $dbh->do('SELECT 1', { mariadb_unknown => 1 }) }, 'attribute "mariadb_unknown" is not accepted for ->do method';
is $dbh->errstr, 'Unknown attribute mariadb_unknown', 'attribute "mariadb_unknown" is not accepted for ->do method';
ok !eval { $dbh->do('SELECT 1', { unknown => 1 }) }, 'attribute "unknown" is not accepted for ->do method';
is $dbh->errstr, 'Unknown attribute unknown', 'attribute "unknown" is not accepted for ->do method';

ok eval { $dbh->prepare('SELECT 1', {}) }, 'empty set of attributes is accepted for ->prepare method';
ok !eval { $dbh->prepare('SELECT 1', { mariadb_unknown => 1 }) }, 'attribute "mariadb_unknown" is not accepted for ->prepare method';
is $dbh->errstr, 'Unknown attribute mariadb_unknown', 'attribute "mariadb_unknown" is not accepted for ->prepare method';
ok !eval { $dbh->prepare('SELECT 1', { unknown => 1 }) }, 'attribute "unknown" is not accepted for ->prepare method';
is $dbh->errstr, 'Unknown attribute unknown', 'attribute "unknown" is not accepted for ->prepare method';

ok eval { $dbh->selectall_arrayref('SELECT 1', {}) }, 'empty set of attributes is accepted for ->selectall_arrayref method';
ok !eval { $dbh->selectall_arrayref('SELECT 1', { mariadb_unknown => 1 }) }, 'attribute "mariadb_unknown" is not accepted for ->selectall_arrayref method';
is $dbh->errstr, 'Unknown attribute mariadb_unknown', 'attribute "mariadb_unknown" is not accepted for ->selectall_arrayref method';
ok !eval { $dbh->selectall_arrayref('SELECT 1', { unknown => 1 }) }, 'attribute "unknown" is not accepted for ->selectall_arrayref method';
is $dbh->errstr, 'Unknown attribute unknown', 'attribute "unknown" is not accepted for ->selectall_arrayref method';

ok !eval { my $tmp = $dbh->{mariadb_unknown} }, 'attribute "mariadb_unknown" cannot be retrieved from database handle';
is $dbh->errstr, 'Unknown attribute mariadb_unknown', 'attribute "mariadb_unknown" cannot be retrieved from database handle';
ok !eval { my $tmp = $dbh->{unknown} }, 'attribute "unknown" cannot be retrieved from database handle';
is $dbh->errstr, 'Unknown attribute unknown', 'attribute "unknown" cannot be retrieved from database handle';

ok !eval { $dbh->{mariadb_unknown} = 1 }, 'attribute "mariadb_unknown" cannot be set on database handle';
is $dbh->errstr, 'Unknown attribute mariadb_unknown', 'attribute "mariadb_unknown" cannot be set on database handle';
ok !eval { $dbh->{unknown} = 1 }, 'attribute "unknown" cannot be set on database handle';
is $dbh->errstr, 'Unknown attribute unknown', 'attribute "mariadb_unknown" cannot be set on database handle';

my $sth = $dbh->prepare('SELECT 1');

ok !eval { my $tmp = $sth->{mariadb_unknown} }, 'attribute "mariadb_unknown" cannot be retrieved from statement handle';
is $sth->errstr, 'Unknown attribute mariadb_unknown', 'attribute "mariadb_unknown" cannot be retrieved from statement handle';
ok !eval { my $tmp = $sth->{unknown} }, 'attribute "unknown" cannot be retrieved from statement handle';
is $sth->errstr, 'Unknown attribute unknown', 'attribute "unknown" cannot be retrieved from statement handle';

ok !eval { $sth->{mariadb_unknown} = 1 }, 'attribute "mariadb_unknown" cannot be set on statement handle';
is $sth->errstr, 'Unknown attribute mariadb_unknown', 'attribute "mariadb_unknown" cannot be set on statement handle';
ok !eval { $sth->{unknown} = 1 }, 'attribute "unknown" cannot be set on statement handle';
is $sth->errstr, 'Unknown attribute unknown', 'attribute "unknown" cannot be set on statement handle';

$dbh->disconnect();
