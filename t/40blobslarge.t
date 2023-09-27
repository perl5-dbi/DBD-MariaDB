use strict;
use warnings;

use Test::More;
use DBI;
use vars qw($test_dsn $test_user $test_password);
use lib '.', 't';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, PrintError => 0 });

plan tests => 15;

# BLOB maximum size on MariaDB is 65,535
# LONGBLOB maximum size is 4 GB
# but effective maximum length of LONGBLOB columns depends 
# configured maximum packet size and available memory.
#
# We test with 512k which should go through on older
# MySQL and MariaDB
my $size = (1024 * 512);

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t40blobslarge"), "Drop table if exists dbd_mysql_t40blobslarge";

my $create = <<EOT;
CREATE TABLE dbd_mysql_t40blobslarge (
    id INT(3) NOT NULL DEFAULT 0,
    name LONGBLOB )
EOT

ok ($dbh->do($create));

my ($blob, $qblob) = "";
my $b = "";

my @out = ( );
my $chr = 0;

# Create little bit random
# byte string
for( 1..$size ) {
    if($chr >= 255)
    {
       $chr = 0;
    }
    
    push @out, chr($chr ++);
}

$blob = join '', @out;

my $sth = undef;

#   Insert a row into the test table.......
my $query = "INSERT INTO dbd_mysql_t40blobslarge VALUES(?, ?)";

ok ($sth = $dbh->prepare( $query ));

# See https://metacpan.org/dist/DBD-MariaDB/view/lib/DBD/MariaDB.pod#Binary-parameters
ok ($sth->bind_param(1, 1, DBI::SQL_INTEGER), "Bind 1 to first param");
ok ($sth->bind_param(2, $blob, DBI::SQL_BINARY), "Bind BLOB to second param");

ok ($sth->execute(), "Execute INSERT statement");

#   Now, try SELECT'ing the row out.
ok ($sth = $dbh->prepare("SELECT * FROM dbd_mysql_t40blobslarge WHERE id = 1"));

ok ($sth->execute(), "Execute SELECT statement");

ok (my $row = $sth->fetchrow_arrayref);

ok (defined($row), "row returned defined");

is (@$row, 2, "records from dbd_mysql_t40blobslarge returned 2");

is ($$row[0], 1, 'id set to 1');

cmp_ok byte_string($$row[1]), 'eq', byte_string($blob), 'Original blob set equal to blob returned';

ok ($dbh->do("DROP TABLE dbd_mysql_t40blobslarge"), "Drop table dbd_mysql_t40blobslarge");

ok ($dbh->disconnect);
