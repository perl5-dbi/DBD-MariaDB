use strict;
use warnings;

use Test::More;
use Test::Deep;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

my $dbh1 = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0 });

plan tests => 12 + 2*2*2*1 + 3 * (2*2*2*13 + 2*2*2 + 2*2*9);

$SIG{__WARN__} = sub { die @_ };

note "Testing use_result attribute";

ok(!$dbh1->{mariadb_use_result});

my $sth1 = $dbh1->prepare('SELECT 1', { mariadb_use_result => 1 });
ok($sth1->{mariadb_use_result});
ok(!$dbh1->{mariadb_use_result});
ok($sth1->execute());
ok($sth1->{mariadb_use_result});
ok(!$dbh1->{mariadb_use_result});

my $dbh2 = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, mariadb_use_result => 1 });
ok($dbh2->{mariadb_use_result});

my $sth2 = $dbh2->prepare('SELECT 1');
ok($sth2->{mariadb_use_result});
ok($dbh2->{mariadb_use_result});
ok($sth2->execute());
ok($sth2->{mariadb_use_result});
ok($dbh2->{mariadb_use_result});

for my $multi_statements (0, 1) {

  my $dbh = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, mariadb_multi_statements => $multi_statements });

  $dbh->do('CREATE TEMPORARY TABLE t(a INT, b INT)');
  $dbh->do('INSERT INTO t VALUES(0, 10)');
  $dbh->do('INSERT INTO t VALUES(1, 20)');
  $dbh->do('INSERT INTO t VALUES(2, 30)');

  for my $async (0, 1) {

    for my $use_result (0, 1) {

      note "Testing with multi_statements=$multi_statements and async=$async and use_result=$use_result ...";

      my $sth = $dbh->prepare($multi_statements ? 'SELECT * FROM t; SELECT * FROM t LIMIT 2 OFFSET 1;' : 'SELECT * FROM t', { mariadb_async => $async });
      $sth->{mariadb_use_result} = $use_result;

      is($sth->rows(), -1);

      for (1..3) {

        if ($async) {
          is($sth->execute(), '0E0');
          for (1..30) {
            last if $sth->mariadb_async_ready();
            sleep 1;
          }
          ok($sth->mariadb_async_ready());
          cmp_deeply([ $sth->mariadb_async_result() ], [ any(-1, 3) ]);
        } else {
          cmp_deeply([ $sth->execute() ], [ any(-1, 3) ]);
        }

        cmp_deeply([ $sth->rows() ], [ any(-1, 3) ]);

        ok($sth->{Active});
        cmp_deeply($sth->fetchrow_arrayref(), [ 0, 10 ]);

        ok($sth->{Active});
        cmp_deeply($sth->fetchrow_arrayref(), [ 1, 20 ]);

        ok($sth->{Active});
        cmp_deeply($sth->fetchrow_arrayref(), [ 2, 30 ]);

        ok(!$sth->fetchrow_arrayref());

        is($sth->rows(), 3);

        if ($multi_statements) {
          ok($sth->{Active});

          ok($sth->more_results());
          cmp_deeply([ $sth->rows() ], [ any(-1, 2) ]);

          ok($sth->{Active});
          cmp_deeply($sth->fetchrow_arrayref(), [ 1, 20 ]);

          ok($sth->{Active});
          cmp_deeply($sth->fetchrow_arrayref(), [ 2, 30 ]);

          ok(!$sth->fetchrow_arrayref());

          is($sth->rows(), 2);
        }

        ok(!$sth->{Active});
        ok(!$sth->more_results());
        ok(!$sth->{Active});

      }

    }

  }

}
