use strict;
use warnings;

use Test::More;
use DBI;
use File::Temp;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

sub fatal_tmpdir_error {
    my $err = $@;
    $err =~ s/ at \S+ line \d+\.?\s*$//;
    $err = 'unknown error' unless $err;
    fatal("Cannot create temporary directory: $err");
    if ( $ENV{CONNECTION_TESTING} ) {
        BAIL_OUT "Cannot create temporary directory: $err";
    } else {
        plan skip_all => "Cannot create temporary directory: $err";
    }
}

sub fatal_connection_error {
    my $err = $@;
    $err =~ s/ at \S+ line \d+\.?\s*$//;
    $err = 'unknown error' unless $err;
    if ( $ENV{CONNECTION_TESTING} ) {
        BAIL_OUT "No connection to embedded server: $err";
    } else {
        diag "No connection to embedded server: $err";
        done_testing;
        exit;
    }
}

sub connect_to_embedded_server {
    my ($tmpdir, $database) = @_;
    my $lang_arg = $ENV{DBD_MARIADB_TESTLANGDIR} ? ",--language=$ENV{DBD_MARIADB_TESTLANGDIR}" : '';
    my $emb_dsn = "DBI:MariaDB:host=embedded;mariadb_embedded_options=--datadir=$tmpdir$lang_arg;";
    $emb_dsn .= "database=$database" if defined $database;
    return eval { DBI->connect($emb_dsn, undef, undef, { RaiseError => 1, PrintError => 0, AutoCommit => 1 }) };
}

sub connect_to_real_server {
    my $dbh = eval { DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 1, PrintError => 0, AutoCommit => 0 }) };
    if (not defined $dbh) {
        my $err = $@;
        $err =~ s/ at \S+ line \d+\.?\s*$//;
        $err = 'unknown error' unless $err;
        if ( $ENV{CONNECTION_TESTING} ) {
            BAIL_OUT "No connection to real non-embedded server: $err";
        } else {
            diag "No connection to real non-embedded server: $err";
        }
    }
    return $dbh;
}

my $tmpdir1 = eval { File::Temp::tempdir(CLEANUP => 1) } or fatal_tmpdir_error();
my $tmpdir2 = eval { File::Temp::tempdir(CLEANUP => 1) } or fatal_tmpdir_error();

my $dbh1 = connect_to_embedded_server($tmpdir1);
plan skip_all => $DBI::errstr if not defined $dbh1 and $DBI::errstr =~ /Embedded server is not supported/;
ok(defined $dbh1, "Connected to embedded server with datadir in $tmpdir1") or fatal_connection_error();
ok($dbh1->do('CREATE DATABASE dbd_mariadb_embedded'), 'Created database');
ok($dbh1->do('USE dbd_mariadb_embedded'), 'Switched to database');

my $dbh2 = connect_to_embedded_server($tmpdir1, 'dbd_mariadb_embedded');
ok(defined $dbh2, "Second connection to embedded server with datadir in $tmpdir1") or fatal_connection_error();

my $dbh3 = connect_to_embedded_server($tmpdir1, 'dbd_mariadb_embedded');
ok(defined $dbh3, "Third conection to embedded server with datadir in $tmpdir1") or fatal_connection_error();

ok($dbh1->do('CREATE TABLE dbd_mariadb_embedded(id INT)'), 'Created table with first connection');
ok($dbh2->do('INSERT INTO dbd_mariadb_embedded(id) VALUES(10)'), 'Inserted values into table with second connection');
is(scalar $dbh1->selectrow_array('SELECT id FROM dbd_mariadb_embedded'), 10, 'Fetched value from table with first connection is correct');
is(scalar $dbh2->selectrow_array('SELECT id FROM dbd_mariadb_embedded'), 10, 'Fetched value from table with second connection is correct');
is(scalar $dbh3->selectrow_array('SELECT id FROM dbd_mariadb_embedded'), 10, 'Fetched value from table with third connection is correct');

ok(!defined connect_to_embedded_server($tmpdir2), "Not connected to different embedded server with datadir in $tmpdir2 while previous connection to embedded server are still active");

SKIP: {
    my $dbh4 = connect_to_real_server();
    skip 'No connection to real non-embedded server', 3 unless defined $dbh4;
    ok(defined $dbh4, 'Connected to real non-embedded server');
    ok(!defined $dbh4->selectrow_array('SHOW TABLES LIKE "dbd_mariadb_embedded"'), 'Real non-embedded server does not have tables from embedded server');
    ok($dbh4->disconnect(), 'Disconnected from real non-embedded server');
}

ok($dbh1->disconnect(), 'Closed first connection to embedded server');
ok($dbh2->disconnect(), 'Closed second connection to embedded server');
ok($dbh3->disconnect(), 'Closed third connection to embedded server');

done_testing;
