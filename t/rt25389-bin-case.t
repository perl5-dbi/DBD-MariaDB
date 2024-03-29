use strict;
use warnings;

use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

use Test::More;

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0 });

if ($dbh->{mariadb_serverversion} < 50100) {
    plan skip_all =>
        "You must have MySQL version 5.1 or greater for this test"
}

plan tests => 8;

my ( $sth, $i );
my @test = qw(AA Aa aa aA);

for my $charset (qw(latin1 utf8)) {
    for my $unique ( "", "unique" ) {

        my $table = "dbd-mysql-$charset-$unique";
        my $create =
"CREATE TEMPORARY TABLE `$table` (name VARCHAR(8) CHARACTER SET $charset COLLATE ${charset}_bin $unique)";

        $dbh->do($create);
        for (@test) {
            $dbh->do("insert into `$table` values ('$_')");
        }
        my $q1 = "select name from `$table`";
        $sth = $dbh->prepare($q1);
        $sth->execute;
        $i = 0;
        while ( my @row = $sth->fetchrow ) {
            $i++;
        }
        is( $i, scalar @test, $q1 );

        my $q2 = "select name from `$table` where "
          . join( " OR ", map { "name = '$_'" } @test );
        $sth = $dbh->prepare($q2);
        $sth->execute;
        $i = 0;
        while ( my @row = $sth->fetchrow ) {
            $i++;
        }
        is( $i, scalar @test, $q2 );
    }
}
