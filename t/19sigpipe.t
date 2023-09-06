use strict;
use warnings;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

plan skip_all => '$SIG{PIPE} is not supported' unless exists $SIG{PIPE};

plan tests => 1;

my $sigpipe_handler_called;
$SIG{PIPE} = sub { $sigpipe_handler_called = 1; };

DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 0, PrintError => 0, AutoCommit => 0 });

kill('PIPE', $$);

ok($sigpipe_handler_called, 'DBI->connect() does not overwrite $SIG{PIPE} handler');
