use strict;
use warnings;

use Config;

use Test::More;
use DBI;

use vars qw($test_dsn $test_user $test_password);
use lib 't', '.';
require 'lib.pl';

# This test requires real fork(). Windows thread-based fork() is not suitable
# because DBI itself does not support sharing $dbh handles between threads.
plan skip_all => 'real fork() is not supported' unless $Config{d_fork};

my $dbh = DbiTestConnect($test_dsn, $test_user, $test_password, { RaiseError => 0, PrintError => 1 });

sub reconnect {
  note 'Reconnecting...';
  $dbh = DBI->connect($test_dsn, $test_user, $test_password, { RaiseError => 0, PrintError => 1 });
  ok($dbh);
}

plan tests => 92;

for my $with_fork (0, 1) {
  note 'Testing $dbh connection ' . ($with_fork ? 'with fork and InactiveDestroy=1' : 'without fork');
  if ($with_fork) {
    my $pid = fork();
    if ($pid == 0) {
      $dbh->{RaiseError} = 1;
      # $dbh with InactiveDestroy=1 is not auto disconnected when process exits
      $dbh->{InactiveDestroy} = 1;
      die unless $dbh->{Active};
      exit;
    }
    ok(defined $pid);
    waitpid($pid, 0);
    is($?, 0);
  }
  ok($dbh->{Active});
  ok(!$dbh->{RaiseError});
  ok(!$dbh->{InactiveDestroy});
  ok($dbh->ping());
  ok($dbh->{Active});

  note 'Testing explicit undef $dbh connection ' . ($with_fork ? 'with fork and InactiveDestroy=1' : 'without fork');
  if ($with_fork) {
    my $pid = fork();
    if ($pid == 0) {
      $dbh->{RaiseError} = 1;
      # $dbh with InactiveDestroy=1 is not auto disconnected when process exits
      $dbh->{InactiveDestroy} = 1;
      die unless $dbh->{Active};
      # Explicitly undef $dbh
      undef $dbh;
      exit;
    }
    ok(defined $pid);
    waitpid($pid, 0);
    is($?, 0);
  }
  ok($dbh->{Active});
  ok(!$dbh->{RaiseError});
  ok(!$dbh->{InactiveDestroy});
  ok($dbh->ping());
  ok($dbh->{Active});

  if ($with_fork) {
    note 'Testing $dbh connection with fork and without InactiveDestroy=1';
    my $pid = fork();
    if ($pid == 0) {
      $dbh->{RaiseError} = 1;
      # $dbh without InactiveDestroy=1 is auto disconnected when process exits
      die unless $dbh->{Active};
      exit;
    }
    ok(defined $pid);
    waitpid($pid, 0);
    is($?, 0);
    ok(!$dbh->{RaiseError});
    ok(!$dbh->{InactiveDestroy});
    ok(!$dbh->ping());
    TODO: {
      local $TODO = 'Attribute Active is broken.';
      ok(!$dbh->{Active});
    }
    reconnect();
  }

  note 'Testing $sth prepared statement ' . ($with_fork ? 'with fork and InactiveDestroy=1' : 'without fork');
  my $sth = $dbh->prepare('SELECT 1', { mariadb_server_prepare => 1, mariadb_server_prepare_disable_fallback => 1 });
  if ($with_fork) {
    my $pid = fork();
    if ($pid == 0) {
      $dbh->{RaiseError} = 1;
      # $dbh with InactiveDestroy=1 is not auto disconnected when process exits
      $dbh->{InactiveDestroy} = 1;
      # $sth with InactiveDestroy=1 is not auto destroyed when process exits
      $sth->{InactiveDestroy} = 1;
      die unless $dbh->{Active};
      exit;
    }
    ok(defined $pid);
    waitpid($pid, 0);
    is($?, 0);
  }
  ok($dbh->{Active});
  ok(!$dbh->{RaiseError});
  ok(!$dbh->{InactiveDestroy});
  ok(!$sth->{InactiveDestroy});
  ok($sth->execute());
  ok($sth->{Active});
  is_deeply([$sth->fetchrow_array()], [1]);

  if ($with_fork) {
    note 'Testing explicit undef $sth prepared statement with fork and InactiveDestroy=1';
    my $pid = fork();
    if ($pid == 0) {
      $dbh->{RaiseError} = 1;
      # $dbh with InactiveDestroy=1 is not auto disconnected when process exits
      $dbh->{InactiveDestroy} = 1;
      # $sth with InactiveDestroy=1 is not auto destroyed when process exits
      $sth->{InactiveDestroy} = 1;
      die unless $dbh->{Active};
      # Explicitly undef $sth
      undef $sth;
      exit;
    }
    ok(defined $pid);
    waitpid($pid, 0);
    is($?, 0);
    ok($dbh->{Active});
    ok(!$dbh->{RaiseError});
    ok(!$dbh->{InactiveDestroy});
    ok(!$sth->{InactiveDestroy});
    ok($sth->execute());
    ok($sth->{Active});
    is_deeply([$sth->fetchrow_array()], [1]);
  }

  if ($with_fork) {
    note 'Testing $sth prepared statement with fork and without InactiveDestroy=1';
    my $pid = fork();
    if ($pid == 0) {
      $dbh->{RaiseError} = 1;
      # $dbh with InactiveDestroy=1 is not auto disconnected when process exits
      $dbh->{InactiveDestroy} = 1;
      # $sth without InactiveDestroy=1 is auto destroyed when process exits
      die unless $dbh->{Active};
      exit;
    }
    ok(defined $pid);
    waitpid($pid, 0);
    is($?, 0);
    ok($dbh->{Active});
    ok(!$dbh->{RaiseError});
    ok(!$dbh->{InactiveDestroy});
    ok(!$sth->{InactiveDestroy});
    $sth->{PrintError} = 0; # next execute prints expected error, mute it
    ok(!$sth->execute());
    ok(!$sth->{Active});
  }

  undef $sth;

  note 'Testing $dbh->disconnect() ' . ($with_fork ? 'with fork' : 'without fork');
  if ($with_fork) {
    my $pid = fork();
    if ($pid == 0) {
      # InactiveDestroy=1 has no effect on $dbh->disconnect()
      $dbh->{RaiseError} = 1;
      $dbh->{InactiveDestroy} = 1;
      die unless $dbh->{Active};
      die unless $dbh->disconnect();
      die unless !$dbh->{Active};
      exit;
    }
    ok(defined $pid);
    waitpid($pid, 0);
    is($?, 0);
  } else {
    ok($dbh->{Active});
    ok($dbh->disconnect());
    ok(!$dbh->{Active});
  }
  ok(!$dbh->{RaiseError});
  ok(!$dbh->{InactiveDestroy});
  ok(!$dbh->ping());
  TODO: {
    local $TODO = 'Attribute Active is broken.' if $with_fork;
    ok(!$dbh->{Active});
  }
  reconnect();

  if ($with_fork) {
    note 'Testing DBI->disconnect_all() with fork';
    my $pid = fork();
    if ($pid == 0) {
      # DBI->disconnect_all() disconnects also $dbh handles marked with InactiveDestroy=1
      $dbh->{RaiseError} = 1;
      $dbh->{InactiveDestroy} = 1;
      die unless $dbh->{Active};
      DBI->disconnect_all();
      die unless !$dbh->{Active};
      exit;
    }
    ok(defined $pid);
    waitpid($pid, 0);
    is($?, 0);
    ok(!$dbh->{RaiseError});
    ok(!$dbh->{InactiveDestroy});
    ok(!$dbh->ping());
    TODO: {
      local $TODO = 'Attribute Active is broken.';
      ok(!$dbh->{Active});
    }
    reconnect();
  }
}

# This must be the last test because some MariaDB and MySQL clients
# are broken and after calling DBI->disconnect_all() it is not
# possible to open a new connection.
note 'Testing DBI->disconnect_all() without fork';
ok($dbh->{Active});
DBI->disconnect_all();
ok(!$dbh->{Active});
ok(!$dbh->{RaiseError});
ok(!$dbh->{InactiveDestroy});
ok(!$dbh->ping());
ok(!$dbh->{Active});
