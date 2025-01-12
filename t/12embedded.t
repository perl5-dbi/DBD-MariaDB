use strict;
use warnings;

use Test::More;
use DBI;
use DBD::MariaDB;
use File::Temp;

use vars qw($test_dsn $test_user $test_password $test_emboptions);
use lib 't', '.';
require 'lib.pl';

sub get_error {
    my $err = $@;
    $err =~ s/ at \S+ line \d+\.?\s*$//;
    $err = 'unknown error' unless $err;
    return $err;
}

sub fatal_connection_error {
    my ($type) = @_;
    my $err = get_error();
    if ( $ENV{CONNECTION_TESTING} ) {
        BAIL_OUT "No connection to $type server: $err";
    } else {
        diag "No connection to $type server: $err";
        done_testing;
        exit;
    }
}

sub connect_to_embedded_server {
    my ($tmpdir, $database) = @_;
    my $emb_dsn = "DBI:MariaDB:host=embedded;mariadb_embedded_options=--datadir=$tmpdir";
    $emb_dsn .= ",$test_emboptions" if length $test_emboptions;
    $emb_dsn .= ";database=$database" if defined $database;
    return eval { DBI->connect($emb_dsn, undef, undef, { RaiseError => 1, PrintError => 0 }) };
}

sub connect_to_real_server {
    return eval { DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, AutoCommit => 0 }) };
}

sub test_dsn_uses_embedded_server {
    my ($dbi_dsn, $driver_dsn) = ($test_dsn =~ /^([^:]*:[^:]*:)(.*)$/);
    my $attr_dsn = DBD::MariaDB->parse_dsn($driver_dsn);
    return exists $attr_dsn->{host} && $attr_dsn->{host} eq 'embedded';
}

my $tmpdir1 = File::Temp::tempdir(CLEANUP => 1);
my $tmpdir2 = File::Temp::tempdir(CLEANUP => 1);

my $dbh1 = connect_to_embedded_server($tmpdir1);
plan skip_all => $DBI::errstr if not defined $dbh1 and $DBI::errstr =~ /Embedded server is not supported/;
ok(defined $dbh1, "Connected to embedded server with datadir in $tmpdir1") or fatal_connection_error('embedded');
ok($dbh1->do('CREATE DATABASE dbd_mariadb_embedded'), 'Created database');
ok($dbh1->do('USE dbd_mariadb_embedded'), 'Switched to database');

my $dbh2 = connect_to_embedded_server($tmpdir1, 'dbd_mariadb_embedded');
ok(defined $dbh2, "Second connection to embedded server with datadir in $tmpdir1") or fatal_connection_error('embedded');

my $dbh3 = connect_to_embedded_server($tmpdir1, 'dbd_mariadb_embedded');
ok(defined $dbh3, "Third connection to embedded server with datadir in $tmpdir1") or fatal_connection_error('embedded');

ok($dbh1->do('CREATE TABLE dbd_mariadb_embedded(id INT)'), 'Created table with first connection');
ok($dbh2->do('INSERT INTO dbd_mariadb_embedded(id) VALUES(10)'), 'Inserted values into table with second connection');
is(scalar $dbh1->selectrow_array('SELECT id FROM dbd_mariadb_embedded'), 10, 'Fetched value from table with first connection is correct');
is(scalar $dbh2->selectrow_array('SELECT id FROM dbd_mariadb_embedded'), 10, 'Fetched value from table with second connection is correct');
is(scalar $dbh3->selectrow_array('SELECT id FROM dbd_mariadb_embedded'), 10, 'Fetched value from table with third connection is correct');

ok(!defined connect_to_embedded_server($tmpdir2), "Not connected to different embedded server with datadir in $tmpdir2 while previous connection to embedded server are still active");

SKIP: {
    skip 'No connection to real non-embedded server: Test suite uses embedded server only', 3 if test_dsn_uses_embedded_server();
    my $dbh4 = connect_to_real_server();
    skip 'No connection to real non-embeeded server: ' . get_error(), 3 if not defined $dbh4 and not $ENV{CONNECTION_TESTING};
    ok(defined $dbh4, 'Connected to real non-embedded server') or fatal_connection_error('real non-embedded');
    ok(!defined $dbh4->selectrow_array('SHOW TABLES LIKE "dbd_mariadb_embedded"'), 'Real non-embedded server does not have tables from embedded server');
    ok($dbh4->disconnect(), 'Disconnected from real non-embedded server');
}

ok($dbh1->disconnect(), 'Closed first connection to embedded server');
ok($dbh2->disconnect(), 'Closed second connection to embedded server');
ok($dbh3->disconnect(), 'Closed third connection to embedded server');

done_testing;
