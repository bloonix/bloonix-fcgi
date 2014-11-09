package Bloonix::FCGI;

use strict;
use warnings;
use IO::Socket;
use JSON;
use FCGI;
use Params::Validate qw();
use POSIX qw(:sys_wait_h);
use Log::Handler;
use Time::HiRes qw();
use Bloonix::FCGI::SharedFile;
use Bloonix::FCGI::Request;
use base qw(Bloonix::Accessor);
use constant PARENT_PID => $$;

__PACKAGE__->mk_accessors(qw/socket request done children ipc log json/);
__PACKAGE__->mk_counters(qw/ttlreq ttlbyt/);

our $VERSION = "0.3";

sub new {
    my $class = shift;
    my $opts = $class->validate(@_);
    my $self = bless $opts, $class;

    $self->init;
    $self->ipc->locking(1);
    $self->daemonize;

    return $self;
}

sub init {
    my $self = shift;

    $self->{log} = Log::Handler->get_logger("bloonix");

    $self->log->info("start socket on port :$self->{port}");
    $self->{socket} = FCGI::OpenSocket(
        ":".$self->{port},
        $self->{listen},
    );

    my $oldfh = select STDOUT;
    $| = 1;
    select STDERR;
    $| = 1;
    select $oldfh;

    $self->{request} = FCGI::Request(
        \*STDIN,
        \*STDOUT,
        \*STDERR,
        \%ENV,
        $self->socket,
        &FCGI::FAIL_ACCEPT_ON_INTR
    );

    $self->{ipc} = Bloonix::FCGI::SharedFile->new($self->{max_servers}, $self->{lockfile});
    $self->{done} = 0;
    $self->{children} = { };
    $self->{to_reap} = { };
    $self->{kill_procs} = 0;
    $self->{next_kill} = time;
    $self->{json} = JSON->new->utf8;
}

sub daemonize {
    my $self = shift;
    my $reap = $self->{to_reap};

    # Bloonix server-status:
    # "S" Starting up, "W" Waiting for connection, 
    # "R" Reading request, "P" Processing request,
    # "N" No request received

    $SIG{CHLD} = sub { $self->sig_chld_handler(@_) };

    foreach my $sig (qw/HUP INT TERM PIPE/) {
        $SIG{$sig} = sub {
            $self->log->warning("signal $sig received");
            $self->done(1);
        };
    }

    while ($self->done == 0) {
        # Reap timed out children.
        $self->reap_children;

        # Process status counter
        my ($idle, $running, $total) = (0, 0, 0);

        # Each idle process is stored, because if there are too much
        # processes running, then sig-term is send only to processes
        # that are currently in idle state. Yes, there is a race condition,
        # but it's better to try to kill only processes in idle state as
        # to try any process. @idle is passed to kill_child.
        my @idle;

        # Count the status of all processes.
        my %status = (S => 0, W => 0, R => 0, P => 0, N => 0);
        my %pidstatus;

        foreach my $pid (keys %{$self->children}) {
            my $process = $self->ipc->get($pid)
                or next;

            $status{$process->{status}}++;
            $pidstatus{$pid} = $process->{status};

            if ($process->{status} eq "W" || $process->{status} eq "S") {
                push @idle, $pid;
                $idle++;
            } else {
                $running++;

                # Kill process that are running to long.
                if ($process->{status} eq "P" && $process->{time} + $self->{timeout} <= time) {
                    $self->log->notice("process $$ runs on a timeout - kill hard");
                    kill 9, $pid;
                }
            }

            $total++;
        }

        # Log a total process status.
        $self->log->debug(join(", ", map { "$_:$pidstatus{$_}" } sort keys %pidstatus));
        $self->log->debug("S[$status{S}] W[$status{W}] R[$status{R}] P[$status{P}] N[$status{N}]");

        # Kill children if max_spare_servers was reached
        # and try only to kill children in idle state.
        $self->kill_children(@idle);

        if ($idle >= $self->{max_spare_servers}) {
            $self->{kill_procs} = 1
        } elsif ($idle < $self->{min_spare_servers}) {
            $self->{kill_procs} = 0;

            if ($total < $self->{max_servers}) {
                my $to_spawn = $self->{min_spare_servers} - $idle;
                $self->log->info("min_spare_servers reached - spawn $to_spawn processes");
            
                for (1 .. $to_spawn) {
                    my $slot = $self->ipc->get_free_slot;

                    if (defined $slot) {
                        my $pid = fork;

                        if ($pid) {
                            $self->log->info("spawn server process $pid - slot $slot");
                            $self->log->info("ipc left slots:", $self->ipc->freeslots);
                            $self->ipc->init_free_slot($slot => $pid);
                            $self->children->{$pid} = $pid;
                        } elsif (!defined $pid) {
                            die "unable to fork() - $!";
                        } else {
                            $SIG{CHLD} = "DEFAULT";
                            foreach my $sig (qw/HUP INT TERM PIPE/) {
                                $SIG{$sig} = sub {
                                    $self->log->warning("signal $sig received");
                                    $self->done(1);
                                };
                            }
                            $self->ipc->locking(0);
                            $self->ipc->wait_for_slot($slot => $$);
                            return;
                        }
                    } else {
                        $self->log->warning("no free slots available");
                    }
                }
            } else {
                $self->log->warning("max_servers of $self->{max_servers} reached");
            }
        }

        $self->log->debug("$idle processed idle, $running processes running");
        Time::HiRes::usleep(200_000);
    }

    $self->stop_server;
    $self->ipc->destroy;
    exit 0;
}

sub kill_children {
    my ($self, @idle) = @_;
    my $reap = $self->{to_reap};

    # Nothing to do.
    if (!$self->{kill_procs}) {
        return;
    }

    # There are no idle processes or the count of
    # idle processes is already equal the minimum
    # count of spare servers.
    if (!@idle || @idle <= $self->{min_spare_servers}) {
        $self->{kill_procs} = 0;
        return;
    }

    # Killing cpu friendly.
    if ($self->{next_kill} > time) {
        return;
    }

    $self->log->info(
        "max spare servers were reached - kill 1 process,",
        @idle - $self->{min_spare_servers}, "left"
    );

    foreach my $pid (@idle) {
        if (!exists $reap->{$pid}) {
            # Kill only one child per second.
            $self->{next_kill} = time + 1;
            # A timeout is stored. If the process doesn't died
            # within the timeout, the process will be killed hard
            # in reap_children.
            $reap->{$pid} = time + $self->{timeout};
            kill 15, $pid;
            # We killing only one process at one time.
            last;
        }
    }
}

sub reap_children {
    my $self = shift;
    my $reap = $self->{to_reap};

    foreach my $pid (keys %$reap) {
        if ($reap->{$pid} <= time) {
            $self->log->notice("process $pid runs on a reap timeout - kill hard");
            kill 9, $pid;
        }
    }
}

sub stop_server {
    my $self = shift;
    my @chld = keys %{$self->children};
    my $wait = 15;

    if (!@chld) {
        return;
    }

    # Kill soft
    kill 15, @chld;

    while ($wait-- && @chld) {
        sleep 1;
        @chld = keys %{$self->children};
    }

    if (@chld) {
        # Kill hard
        kill 9, @chld;
    }
}

sub sig_chld_handler {
    my $self = shift;
    my $children = $self->children;
    my $reap = $self->{to_reap};

    while ((my $child = waitpid(-1, WNOHANG)) > 0) {
        if ($? > 0) {
            $self->log->error("child $child died: $?");
        } else {
            $self->log->notice("child $child died: $?");
        }

        $self->ipc->remove($child);
        $self->log->info("ipc free slots:", $self->ipc->freeslots);
        delete $children->{$child};
        delete $reap->{$child};
    }

    $SIG{CHLD} = sub { $self->sig_chld_handler(@_) };
}

sub proc_status {
    my $self = shift;

    return $self->ipc->proc_status;
}

sub statm {
    my ($self, $pid) = @_;

    if (!$self->{statm} || $pid) {
        $pid //= $$;

        my $file = "/proc/$pid/statm";
        my %stat = ();

        open my $fh, '<', $file or return undef;
        my @line = split /\s+/, <$fh>;
        close($fh);

        @{$self->{statm}}{qw(size resident share trs lrs drs dtp)} = map { $_ * 4096 } @line;
    }

    return $self->{statm};
}

sub accept {
    my $self = shift;
    my $server_status = $self->{server_status};

    # Reset memory statistics
    $self->{statm} = undef;

    if ($self->done) {
        exit 0;
    }

    if ($self->{max_requests} && $self->ttlreq > $self->{max_requests}) {
        $self->log->info("$$ reached max_requests of $self->{max_requests} - good bye");
        exit 0;
    }

    $self->ipc->set($$, status => "W", time => time);

    while ($self->done == 0 && $self->request->Accept() >= 0) {
        $self->ipc->set($$, status => "R", time => time);
        my $req = Bloonix::FCGI::Request->new();

        $self->ipc->set($$,
            status  => "P",
            ttlreq  => $self->ttlreq(1),
            client  => $req->remote_addr,
            request => join(" ", $req->request_method, $req->request_uri),
        );

        if ($server_status->{enabled} eq "yes" && $req->path_info eq $server_status->{location}) {
            my $pretty = $req->param("pretty") // "false";
            my $json = $pretty eq "true" ? $self->json->pretty : $self->json;
            my $addr = $req->remote_addr || "n/a";
            my $allow_from = $server_status->{allow_from};

            if ($allow_from->{all} || $allow_from->{$addr}) {
                print "Content-Type: application/json\n\n";
                print $json->encode({ status => "ok", data => $self->ipc->proc_status });
                $self->log->info("server status request from $addr - access allowed");
                next;
            }

            $self->log->warning("server status request from $addr - access denied");
        }

        return $req;
    }

    if ($self->done) {
        exit 0;
    }

    $self->ipc->set($$, status => "N", time => time);
    return undef;
}

sub attach {
    my $self = shift;

    return $self->request->Attach();
}

sub detach {
    my $self = shift;

    return $self->request->Detach();
}

sub finish {
    my $self = shift;

    return $self->request->Finish();
}

sub validate {
    my $class = shift;

    my %options = Params::Validate::validate(@_, {
        min_spare_servers => {
            type  => Params::Validate::SCALAR,
            regex => qr/^\d+\z/,
            default => 10,
        },
        max_spare_servers => {
            type  => Params::Validate::SCALAR,
            regex => qr/^\d+\z/,
            default => 20,
        },
        max_servers => {
            type  => Params::Validate::SCALAR,
            regex => qr/^\d+\z/,
            default => 50,
        },
        max_requests => {
            type  => Params::Validate::SCALAR,
            regex => qr/^\d+\z/,
            default => 0,
        },
        server_status => {
            type => Params::Validate::HASHREF,
            default => { },
        },
        timeout => {
            type  => Params::Validate::SCALAR,
            regex => qr/^\d+\z/,
            default => 300,
        },
        port => {
            type => Params::Validate::SCALAR,
            default => 9000,
        },
        listen => {
            type => Params::Validate::SCALAR,
            default => SOMAXCONN,
        },
        lockfile => {
            type => Params::Validate::SCALAR,
            default => "/var/cache/bloonix/blxipc.%P.lock",
        }
    });

    $options{server_status} = $class->validate_server_status(
        $options{server_status}
    );

    return \%options;
}

sub validate_server_status {
    my $class = shift;

    my %options = Params::Validate::validate(@_, {
        enabled => {
            type => Params::Validate::SCALAR,
            default => "yes",
            regex => qr/^(yes|no)\z/
        },
        location => {
            type => Params::Validate::SCALAR,
            default => "/server-status"
        },
        allow_from => {
            type => Params::Validate::SCALAR,
            default => "127.0.0.1"
        },
        #auth_string => {
        #    type => Params::Validate::SCALAR,
        #    optional => 1,
        #}
    });

    $options{allow_from} =~ s/\s//g;
    $options{allow_from} = {
        map { $_, 1 } split(/,/, $options{allow_from})
    };

    return \%options;
}

sub DESTROY {
    if ($$ == PARENT_PID) {
        my $self = shift;
        my $socket = $self->{socket};
        FCGI::CloseSocket($socket);
    }
}

1;

=head1 NAME

Bloonix::FCGI - The database interface.

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 METHODS

=head1 EXPORTS

No exports.

=head1 REPORT BUGS

Please report all bugs to <support(at)bloonix.de>.

=head1 AUTHOR

Jonny Schulz <support(at)bloonix.de>.

=head1 COPYRIGHT

Copyright (C) 2009-2014 by Jonny Schulz. All rights reserved.

=cut
