use strict;
use warnings;

use Test::More;

use DBI;
use DBD::MariaDB;

BEGIN {
    eval {
        require Net::SSLeay;
        Net::SSLeay->VERSION(1.43); # for Net::SSLeay::initialize()
    } or do {
        plan skip_all => 'Net::SSLeay 1.43 is required for this test';
    };
    Net::SSLeay->import();
}

sub diag_ssl {
    my $error = Net::SSLeay::ERR_get_error();
    my $error_string = Net::SSLeay::ERR_error_string($error);
    diag($error_string);
    Net::SSLeay::load_error_strings();
    my $error_string2 = Net::SSLeay::ERR_error_string($error);
    diag($error_string2);
}

Net::SSLeay::initialize();

my $ctx_new_sub = Net::SSLeay->can('CTX_tlsv1_2_new') || Net::SSLeay->can('CTX_tlsv1_1_new') || Net::SSLeay->can('CTX_tlsv1_new');
plan skip_all => 'Net::SSLeay does not provide TLS context' unless defined $ctx_new_sub;

plan tests => 4;

my $ctx1 = $ctx_new_sub->();
ok($ctx1, 'Net::SSLeay TLS context was created before MariaDB connection') or diag_ssl();

my $dbh = DBI->connect('DBI:MariaDB:', undef, undef, { RaiseError => 0, PrintError => 0 });
$dbh->disconnect() if defined $dbh;
$dbh = undef;
pass('MariaDB connection was successfully created and destroyed');

my $ctx2 = $ctx_new_sub->();
ok($ctx2, 'Net::SSLeay TLS context was created after MariaDB connection') or diag_ssl();

Net::SSLeay::CTX_free($ctx1);
Net::SSLeay::CTX_free($ctx2);
pass('Net::SSLeay TLS contexts were destroyed');
