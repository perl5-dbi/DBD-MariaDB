use strict;
use warnings;

use DBI;
use DBI::Const::GetInfoType;
use Encode;
use Test::More;

my $tb = Test::More->builder;
binmode $tb->output,         ":utf8";
binmode $tb->failure_output, ":utf8";
binmode $tb->todo_output,    ":utf8";

use vars qw($test_dsn $test_user $test_password);
use lib '.', 't';
require 'lib.pl';

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
  { RaiseError => 1, PrintError => 0 });

if ($dbh->get_info($GetInfoType{SQL_DBMS_NAME}) eq 'MySQL' and $dbh->{mariadb_serverversion} < 50708) {
    plan skip_all => "MySQL servers < 5.7.8 do not JSON data type";
}

if ($dbh->get_info($GetInfoType{SQL_DBMS_NAME}) eq 'MariaDB' and $dbh->{mariadb_serverversion} < 100207) {
    plan skip_all => "MariaDB servers < 10.2.7 do not JSON data type";
}

plan tests => 19;

ok ($dbh->do("DROP TABLE IF EXISTS dbd_mysql_json1"), 'Drop table if exists dbd_mysql_json1' );

ok ($dbh->do('CREATE TABLE dbd_mysql_json1 (my_json JSON)'), 'Create table dbd_mysql_json1' );

ok ($dbh->do("INSERT INTO dbd_mysql_json1 SET my_json = JSON_OBJECT('drink', CONVERT(UNHEX('537AC5916CC5916CC3A9') USING utf8mb4))"), 'Insert example JSON data in hexadecumal form' );

ok (my $sth = $dbh->prepare("INSERT INTO dbd_mysql_json1 VALUES (?)"), 'Prepare insert for JSON data in string form' );
my $input_in_bytes = "{\"drink\": \"Sz\xC5\x91l\xC5\x91l\xC3\xA9\"}";
my $input = Encode::decode('UTF-8', $input_in_bytes);
ok ($sth->execute($input), 'Execute prepared statement with JSON data in string form' );

ok ($sth = $dbh->prepare("SELECT my_json FROM dbd_mysql_json1"), 'Prepare select statement in string form' );

ok ($sth->execute, 'Execute prepared statement' );

ok (my $result = $sth->fetchall_arrayref, 'Fetch JSON data in string form' );

ok (defined($result), 'Result returned defined' );

my $output_in_bytes = "{\"drink\": \"Sz\xC5\x91l\xC5\x91l\xC3\xA9\"}";
my $output = Encode::decode('UTF-8', $output_in_bytes);
if ($dbh->get_info($GetInfoType{SQL_DBMS_NAME}) eq 'MySQL') {
	# XXX MySQL returns octets instead of internal Perl Unicode
	# DBD::MySQL issue: https://github.com/perl5-dbi/DBD-MariaDB/issues/142
	# MySQL issue: https://bugs.mysql.com/bug.php?id=95698
	is ($result->[0][0], $output_in_bytes, "Should be $output" );
	is ($result->[1][0], $output_in_bytes, "Should be $output" );
} else {
	is ($result->[0][0], $output, "Should be $output");
	is ($result->[1][0], $output, "Should be $output");
}

ok ($sth = $dbh->prepare("SELECT HEX(my_json) FROM dbd_mysql_json1"), 'Prepare select statement in hexadecimal form' );

ok ($sth->execute, 'Execute prepared statement' );

ok ($result = $sth->fetchall_arrayref, "Fetch data" );

ok (defined($result), "Result returned defined" );

is ($result->[0][0], '7B226472696E6B223A2022537AC5916CC5916CC3A9227D', 'Fetch JSON data in hexadecumal form'  );
is ($result->[1][0], '7B226472696E6B223A2022537AC5916CC5916CC3A9227D', 'Fetch JSON data in hexadecumal form'  );

ok ($dbh->do("DROP TABLE dbd_mysql_json1"), "Drop table dbd_mysql_json1" );

ok ($dbh->disconnect, "Disconnect" );
