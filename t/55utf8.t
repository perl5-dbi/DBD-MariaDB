use strict;
use warnings FATAL => 'all';

use DBI;
use Test::More;
use vars qw($test_dsn $test_user $test_password);
use vars qw($COL_NULLABLE $COL_KEY);
use lib 't', '.';
require 'lib.pl';

my $tb = Test::More->builder;
binmode $tb->output,         ":utf8";
binmode $tb->failure_output, ":utf8";
binmode $tb->todo_output,    ":utf8";

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

plan tests => 40 * 2;

for my $mariadb_server_prepare (0, 1) {
$dbh= DBI->connect("$test_dsn;mariadb_server_prepare=$mariadb_server_prepare;mariadb_server_prepare_disable_fallback=1", $test_user, $test_password,
                      { RaiseError => 1, PrintError => 0, AutoCommit => 0 });

ok $dbh->do("DROP TABLE IF EXISTS dbd_mysql_t55utf8");

my $create =<<EOT;
CREATE TABLE dbd_mysql_t55utf8 (
    name VARCHAR(64) CHARACTER SET utf8,
    bincol BLOB,
    shape GEOMETRY,
    binutf VARCHAR(64) CHARACTER SET utf8 COLLATE utf8_bin,
    profile TEXT CHARACTER SET utf8,
    str2 VARCHAR(64) CHARACTER SET utf8,
    ascii VARCHAR(64) CHARACTER SET latin1,
    latin VARCHAR(64) CHARACTER SET latin1
)
EOT

ok $dbh->do($create);

my $unicode_str        = "\N{U+0100}dam";   # Unicode "Adam" with a macron (internally stored as utf8)
my $quoted_unicode_str = "'\N{U+0100}dam'";

my $blob               = "\x{c4}\x{80}dam"; # UTF-8 representation of $unicode_str
my $quoted_blob        = "'\x{c4}\x{80}dam'";

my $unicode_str2       = "\x{c1}dam";       # Unicode "Adam" with a acute (internally stored as latin1)
my $ascii_str          = "?dam";            # ASCII representation of $unicode_str (and also $unicode_str2)
my $latin1_str2        = "\x{c1}dam";       # Latin1 representation of $unicode_str2 (well, really same as $unicode_str2)
my $blob2              = "\x{c3}\x{81}dam"; # UTF-8 representation of $unicode_str2

cmp_ok $dbh->quote($unicode_str), 'eq', $quoted_unicode_str, 'testing quoting of utf 8 string';

cmp_ok $dbh->quote($blob), 'eq', $quoted_blob, 'testing quoting of blob';

ok $dbh->do("SET SQL_MODE=''"), 'SET SQL_MODE=\'\'';

# GeomFromText() is deprecated as of MySQL 5.7.6, use ST_GeomFromText() instead
my $geomfromtext = $dbh->{mariadb_serverversion} >= 50706 ? 'ST_GeomFromText' : 'GeomFromText';

my $query = <<EOI;
INSERT INTO dbd_mysql_t55utf8 (name, bincol, shape, binutf, profile, str2, ascii, latin)
    VALUES (?, ?, $geomfromtext('Point(132865 501937)'), ?, ?, ?, ?, ?)
EOI

# Do not use prepared statements because ST_GeomFromText() is not supported
# With SET SQL_MODE='' is mariadb_server_prepare_disable_fallback not working
# And without SET SQL_MODE='' below 'Incorrect string value' are fatal errors, not warnings...
my $sth = $dbh->prepare($query, { mariadb_server_prepare => 0 }) or die "$DBI::errstr";
ok $sth->bind_param(1, $unicode_str);
ok $sth->bind_param(2, $blob, DBI::SQL_BINARY);
ok $sth->bind_param(3, $unicode_str);
ok $sth->bind_param(4, $unicode_str);
ok $sth->bind_param(5, $unicode_str2);
ok $sth->bind_param(6, $unicode_str);
ok $sth->bind_param(7, $unicode_str2);
ok $sth->execute() or die("Execute failed: ".$DBI::errstr);

cmp_ok($dbh->{mariadb_warning_count}, '==', 1, 'got warning for INSERT') or do { diag("SHOW WARNINGS:"); diag($_->[2]) foreach $dbh->selectall_array("SHOW WARNINGS", { mariadb_server_prepare => 0 }); };
my (undef, undef, $warning) = $dbh->selectrow_array("SHOW WARNINGS", { mariadb_server_prepare => 0 });
like($warning, qr/^(?:Incorrect string value: '\\xC4\\x80dam'|Data truncated) for column (?:'ascii'|`.*`\.`.*`\.`ascii`) at row 1$/, 'warning is correct');

# AsBinary() is deprecated as of MySQL 5.7.6, use ST_AsBinary() instead
my $asbinary = $dbh->{mariadb_serverversion} >= 50706 ? 'ST_AsBinary' : 'AsBinary';

$query = "SELECT name,bincol,$asbinary(shape), binutf, profile, str2, ascii, latin FROM dbd_mysql_t55utf8 LIMIT 1";
$sth = $dbh->prepare($query) or die "$DBI::errstr";

ok $sth->execute;

my $ref;
$ref = $sth->fetchrow_arrayref ;

ok defined $ref, 'got data' or $ref = [];

cmp_ok $ref->[0], 'eq', $unicode_str;
cmp_ok $ref->[1], 'eq', $blob;
cmp_ok $ref->[3], 'eq', $unicode_str;
cmp_ok $ref->[4], 'eq', $unicode_str;
cmp_ok $ref->[5], 'eq', $unicode_str2;
cmp_ok $ref->[6], 'eq', $ascii_str;
cmp_ok $ref->[7], 'eq', $latin1_str2;

cmp_ok $ref->[1], 'eq', $blob, "compare $ref->[1] eq $blob";

my $prev_charset = $dbh->selectrow_array('SELECT @@character_set_results');
ok $dbh->do("SET character_set_results='latin1'"), "SET character_set_results='latin1'";
$ref = $dbh->selectrow_arrayref($query);
ok defined $ref, 'got data' or $ref = [];
cmp_ok $ref->[0], 'eq', $ascii_str, 'utf8 data are returned as latin1 when @@character_set_results is latin1';
cmp_ok $ref->[1], 'eq', $blob, 'blob is unchanged when @@character_set_results is latin1';
cmp_ok $ref->[3], 'eq', $ascii_str, 'utf8 data are returned as latin1 when @@character_set_results is latin1';
cmp_ok $ref->[4], 'eq', $ascii_str, 'utf8 data are returned as latin1 when @@character_set_results is latin1';
cmp_ok $ref->[5], 'eq', $latin1_str2, 'utf8 data are returned as latin1 when @@character_set_results is latin1';
cmp_ok $ref->[6], 'eq', $ascii_str, 'latin1 data are returned as latin1 when @@character_set_results is latin1';
cmp_ok $ref->[7], 'eq', $latin1_str2, 'latin1 data are returned as latin1 when @@character_set_results is latin1';

ok $sth = $dbh->prepare("SELECT 1 FROM dbd_mysql_t55utf8 WHERE bincol = ?");
ok !defined eval { $sth->bind_param(1, $unicode_str, DBI::SQL_BINARY) };
like $@, qr/^Wide character in /, '';

ok $dbh->do("SET character_set_results='$prev_charset'"), "SET character_set_results='$prev_charset'";

ok $dbh->do("DROP TABLE dbd_mysql_t55utf8");

ok $dbh->disconnect;
}
