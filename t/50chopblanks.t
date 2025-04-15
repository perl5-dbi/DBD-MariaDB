use strict;
use warnings;

use DBI;
use Test::More;
use lib 't', '.';
require 'lib.pl';

use vars qw($test_dsn $test_user $test_password);

DBI->trace(100) if $ENV{TEST_VERBOSE};

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1 });
$dbh->disconnect;

#plan tests => (6 + ((5 + 8 + 8) * 4)) * 2;
plan tests => 936;

for my $mariadb_server_prepare (0, 1) {
$dbh = DBI->connect("$test_dsn;mariadb_server_prepare=$mariadb_server_prepare;mariadb_server_prepare_disable_fallback=1", $test_user, $test_password,
                      { RaiseError => 1, PrintError => 1 });

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t50chopblanks"), "drop table if exists dbd_mysql_t50chopblanks";

my $create= <<EOT;
CREATE TABLE dbd_mysql_t50chopblanks (
  id INT(4),
  c_text TEXT,
  c_tinytext TINYTEXT,
  c_mediumtext MEDIUMTEXT,
  c_longtext LONGTEXT,
  b_blob BLOB,
  b_tinyblob TINYBLOB,
  b_mediumblob MEDIUMBLOB,
  b_longblob LONGBLOB
)
EOT

ok $dbh->do($create), "create table dbd_mysql_t50chopblanks";

for my $size (1 .. 8) {

my @fields = qw(c_text c_tinytext c_mediumtext c_longtext b_blob b_tinyblob b_mediumblob b_longblob);
my $numfields = scalar @fields;
my $fieldlist = join(', ', @fields);
my $select_fields = join ', ', @fields[0 .. $size - 1];

ok (my $sth= $dbh->prepare("INSERT INTO dbd_mysql_t50chopblanks (id, $fieldlist) VALUES (".('?, ' x $numfields)."?)"));
note('insert prepared');

my $sth2;
eval {
    $sth2 = $dbh->prepare("SELECT $select_fields FROM dbd_mysql_t50chopblanks WHERE id = ?");
    1 } or die $dbh->errstr;
ok($sth2);
note('select prepared');

my $rows;

$rows = [ [1, ''], [2, ' '], [3, ' a b c '], [4, 'blah'] ];

for my $ref (@$rows) {
	my ($id, $value) = @$ref;
	ok $sth->execute($id, ($value) x $numfields), "insert into dbd_mysql_t50chopblanks values ($id ".(", '$value'" x $numfields).")";
	ok $sth2->execute($id), "select $select_fields from dbd_mysql_t50chopblanks where id = $id";

	# First try to retrieve without chopping blanks.
	$sth2->{'ChopBlanks'} = 0;
	my $ret_ref = [];
	ok ($ret_ref = $sth2->fetchrow_arrayref);
	for my $i (0 .. $#{$ret_ref}) {
		cmp_ok $ret_ref->[$i], 'eq', $value, "NoChopBlanks: $fields[$i] should not have blanks chopped";
	}

	# Now try to retrieve with chopping blanks.
	$sth2->{'ChopBlanks'} = 1;

	ok $sth2->execute($id);

	$ret_ref = [];
	ok ($ret_ref = $sth2->fetchrow_arrayref);
	for my $i (0 .. $#{$ret_ref}) {
		my $choppedvalue = $value;
		my $character_field = ($fields[$i] =~ /^c/);
		$choppedvalue =~ s/\s+$// if $character_field; # only chop character, not binary
		cmp_ok $ret_ref->[$i], 'eq', $choppedvalue, "ChopBlanks: $fields[$i] should ".($character_field ? "" : "not ")."have blanks chopped";
	}
    }
}
ok $dbh->do("DROP TABLE dbd_mysql_t50chopblanks"), "drop dbd_mysql_t50chopblanks";
ok $dbh->disconnect;
}
