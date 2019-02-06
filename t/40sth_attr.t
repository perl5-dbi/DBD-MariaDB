use strict;
use warnings;

use Test::More;
use DBI;
use DBD::MariaDB;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });

plan tests => 90;

ok($dbh->do("CREATE TEMPORARY TABLE t(id INT)"));
ok($dbh->do("INSERT INTO t(id) VALUES(1)"));

my $sth1 = $dbh->prepare("SELECT * FROM t");
ok(!$sth1->{Active});
ok($sth1->execute());
ok($sth1->{Active});
is_deeply($sth1->{NAME}, ["id"]);
is_deeply($sth1->fetchall_arrayref(), [ [ 1 ] ]);
ok(!$sth1->{Active});
is_deeply($sth1->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

my $sth2 = $dbh->prepare("SELECT * FROM t", { mariadb_server_prepare => 1 });
ok(!$sth2->{Active});
ok($sth2->execute());
ok($sth2->{Active});
is_deeply($sth2->{NAME}, ["id"]);
is_deeply($sth2->fetchall_arrayref(), [ [ 1 ] ]);
ok(!$sth2->{Active});
is_deeply($sth2->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

ok($dbh->do("INSERT INTO t(id) VALUES(2)"));

my $sth3 = $dbh->prepare("SELECT * FROM t");
ok(!$sth3->{Active});
ok($sth3->execute());
ok($sth3->{Active});
is_deeply($sth3->{NAME}, ["id"]);
is_deeply($sth3->fetchrow_arrayref(), [ 1 ]);
ok($sth3->{Active});
is_deeply($sth3->{NAME}, ["id"]);
is_deeply($sth3->fetchrow_arrayref(), [ 2 ]);
ok(!$sth3->{Active});
is_deeply($sth3->{NAME}, ["id"]);
is_deeply($sth3->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

my $sth4 = $dbh->prepare("SELECT * FROM t", { mariadb_server_prepare => 1 });
ok(!$sth4->{Active});
ok($sth4->execute());
ok($sth4->{Active});
is_deeply($sth4->{NAME}, ["id"]);
is_deeply($sth4->fetchrow_arrayref(), [ 1 ]);
ok($sth4->{Active});
is_deeply($sth4->{NAME}, ["id"]);
is_deeply($sth4->fetchrow_arrayref(), [ 2 ]);
ok(!$sth4->{Active});
is_deeply($sth4->{NAME}, ["id"]);
is_deeply($sth4->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

my $sth5 = $dbh->prepare("SELECT * FROM t");
ok(!$sth5->{Active});
ok($sth5->execute());
ok($sth5->{Active});
is_deeply($sth5->{NAME}, ["id"]);
is_deeply($sth5->fetchrow_arrayref(), [ 1 ]);
ok($sth5->{Active});
is_deeply($sth5->{NAME}, ["id"]);
ok($sth5->finish);
ok(!$sth5->{Active});
is_deeply($sth5->{NAME}, ["id"]);
is_deeply($sth5->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

my $sth6 = $dbh->prepare("SELECT * FROM t", { mariadb_server_prepare => 1 });
ok(!$sth6->{Active});
ok($sth6->execute());
ok($sth6->{Active});
is_deeply($sth6->{NAME}, ["id"]);
is_deeply($sth6->fetchrow_arrayref(), [ 1 ]);
ok($sth6->{Active});
is_deeply($sth6->{NAME}, ["id"]);
ok($sth6->finish);
ok(!$sth6->{Active});
is_deeply($sth6->{NAME}, ["id"]);
is_deeply($sth6->{mariadb_type}, [ DBD::MariaDB::TYPE_LONG ]);

my $sth7 = $dbh->prepare("SELECT NULL as my_name LIMIT 0");
ok(!$sth7->{Active});
ok($sth7->execute());
ok(!$sth7->{Active});
is_deeply($sth7->{NAME}, ["my_name"]);
ok(!$sth7->fetchrow_arrayref());
ok(!$sth7->{Active});
is_deeply($sth7->{mariadb_type}, [ DBD::MariaDB::TYPE_NULL ]);

my $sth8 = $dbh->prepare("SELECT NULL as my_name LIMIT 0");
ok(!$sth8->{Active});
ok($sth8->execute());
ok(!$sth8->{Active});
is_deeply($sth8->{NAME}, ["my_name"]);
ok(!$sth8->fetchrow_arrayref());
ok(!$sth8->{Active});
is_deeply($sth8->{mariadb_type}, [ DBD::MariaDB::TYPE_NULL ]);

my $sth9 = $dbh->prepare("SELECT NULL as my_name LIMIT 0");
ok(!$sth9->{Active});
ok($sth9->execute());
ok(!$sth9->{Active});
is_deeply($sth9->{NAME}, ["my_name"]);
is_deeply($sth9->fetchall_arrayref(), []);
ok(!$sth9->{Active});
is_deeply($sth9->{mariadb_type}, [ DBD::MariaDB::TYPE_NULL ]);

my $sth10 = $dbh->prepare("SELECT NULL as my_name LIMIT 0");
ok(!$sth10->{Active});
ok($sth10->execute());
ok(!$sth10->{Active});
is_deeply($sth10->{NAME}, ["my_name"]);
is_deeply($sth10->fetchall_arrayref(), []);
ok(!$sth10->{Active});
is_deeply($sth10->{mariadb_type}, [ DBD::MariaDB::TYPE_NULL ]);

ok($dbh->disconnect());
