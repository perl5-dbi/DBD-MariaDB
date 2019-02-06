use strict;
use warnings;

use DBI;
use Test::More;
use lib '.', 't';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, PrintError => 0 });

plan tests => 8;

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t32");

my $create = <<EOT;
CREATE TABLE dbd_mysql_t32 (
    id INT(3) PRIMARY KEY NOT NULL,
    name VARCHAR(64))
EOT

ok $dbh->do($create);

my $query = "INSERT INTO dbd_mysql_t32 (id, name) VALUES (?,?)";
ok (my $sth = $dbh->prepare($query));

ok $sth->execute(1, "Jocken");

my $failed = not eval {$sth->execute(1, "Jochen")};
ok $failed, 'fails with duplicate entry';

ok $sth->execute(2, "Jochen");

ok $dbh->do("DROP TABLE dbd_mysql_t32");

ok $dbh->disconnect();
