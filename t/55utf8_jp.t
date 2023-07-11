use strict;
use warnings;

use Test::More;
use DBI;
use Encode;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require "lib.pl";

my $tb = Test::More->builder;
binmode $tb->output,         ":utf8";
binmode $tb->failure_output, ":utf8";
binmode $tb->todo_output,    ":utf8";

sub skip_rt_102404 {
  skip "(Perl 5.13.1 and DBI 1.635) or DBI 1.639 is required due to bug RT 102404", $_[0] unless ($] >= 5.013001 and eval { DBI->VERSION(1.635) }) or eval { DBI->VERSION(1.639) };
}

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { PrintError => 0, RaiseError => 1 });

eval {
  $dbh->do("SET lc_messages = 'ja_JP'");
} or do {
  $dbh->disconnect();
  plan skip_all => "Server lc_messages ja_JP are needed for this test";
};

plan tests => 21;

my $jpnTable = "\N{U+8868}"; # Japanese table
my $jpnGender = "\N{U+6027}\N{U+5225}"; # Japanese word "gender"
my $jpnYamadaTaro = "\N{U+5c71}\N{U+7530}\N{U+592a}\N{U+90ce}"; # a Japanese person name
my $jpnMale = "\N{U+7537}"; # Japanese word "male"
my $jpnErr = qr/\x{4ed8}\x{8fd1}.*\x{884c}\x{76ee}/; # Use \x{...} instead \N{U+...} due to Perl 5.12.0 bug

my $sth;
my $row;

ok($dbh->do("
CREATE TEMPORARY TABLE $jpnTable (
  name VARCHAR(20),
  $jpnGender CHAR(1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
"));

ok($sth = $dbh->prepare("INSERT INTO $jpnTable (name, $jpnGender) VALUES (?, ?)"));
ok($sth->execute($jpnYamadaTaro, $jpnMale));

ok($sth = $dbh->prepare("SELECT * FROM $jpnTable"));
ok($sth->execute());
ok($row = $sth->fetchrow_hashref());

is($row->{name}, $jpnYamadaTaro);
is($row->{$jpnGender}, $jpnMale);
ok(!exists $row->{Encode::encode("UTF-8", $jpnGender)});

is_deeply($sth->{NAME}, [ 'name', $jpnGender ]);
is_deeply($sth->{mariadb_table}, [ $jpnTable, $jpnTable ]);

my $warn;
my $dieerr;
my $dbierr;
my $failed;

$failed = 0;
$dieerr = undef;
$dbierr = undef;
$dbh->{HandleError} = sub { $dbierr = $_[0]; die $_[0]; };
eval {
  $sth = $dbh->prepare("foo");
  $sth->execute();
  1;
} or do {
  $dieerr = $@;
  $failed = 1;
};
$dbh->{HandleError} = undef;

ok($failed);
like($dbierr, $jpnErr);
like($DBI::errstr, $jpnErr);
like($dbh->errstr, $jpnErr);

SKIP : {
  skip_rt_102404 1;
  like($dieerr, $jpnErr);
}

$failed = 0;
$warn = undef;
$dieerr = undef;
$dbh->{PrintError} = 1;
$SIG{__WARN__} = sub { $warn = $_[0] };
eval {
  $sth = $dbh->prepare("foo");
  $sth->execute();
  1;
} or do {
  $dieerr = $@;
  $failed = 1;
};
$dbh->{PrintError} = 0;
$SIG{__WARN__} = 'DEFAULT';

ok($failed);
like($DBI::errstr, $jpnErr);
like($dbh->errstr, $jpnErr);

SKIP : {
  skip_rt_102404 2;
  like($warn, $jpnErr);
  like($dieerr, $jpnErr);
}
