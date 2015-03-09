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

our $VERSION = "0.6";

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
    $SIG{__DIE__} = sub { $self->log->dump(fatal => @_) };
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

    foreach my $sig (qw/USR1 USR2/) {
        $SIG{$sig} = sub {
            my @chld = keys %{$self->{children}};
            $self->log->warning("signal $sig received");
            $self->log->warning("sending $sig to", @chld);
            kill $sig, @chld;
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
                            foreach my $sig (qw/USR1 USR2/) {
                                $SIG{$sig} = sub {
                                    $self->log->warning("signal $sig received - ignoring");
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
        if ($? > 0 && $? != 13) {
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

    # Bloonix::Heaven->accept stops if undef is returned and the outer
    # loop is used to prevent the child to die on SIGUSR1 and SIGUSR2.
    while ($self->done == 0) {
        $self->pre_accept;
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

            if ($self->is_server_status($req)) {
                $self->ipc->set($$, status => "W", time => time);
                next;
            }

            return $req;
        }
    }

    if ($self->done) {
        exit 0;
    }

    $self->ipc->set($$, status => "N", time => time);
    return undef;
}

sub pre_accept {
    my $self = shift;

    # Reset memory statistics
    $self->{statm} = undef;

    if ($self->{max_requests} && $self->ttlreq > $self->{max_requests}) {
        $self->log->warning("$$ reached max_requests of $self->{max_requests} - good bye");
        exit 0;
    }

    if ($self->{max_process_size} && $self->statm && $self->statm->{resident} > $self->{max_process_size}) {
        $self->log->warning(
            "$$ reached max_process_size of",
            $self->{max_process_size_readable},
            sprintf("(%.1fMB)", $self->statm->{resident} / 1048576),
            "- good bye"
        );
        exit 0;
    }
}

sub is_server_status {
    my ($self, $req) = @_;
    my $server_status = $self->{server_status};

    if ($server_status->{enabled} eq "yes" && $req->path_info eq $server_status->{location}) {
        my $authkey = $req->param("authkey") // "";
        my $addr = $req->remote_addr || "n/a";
        my $allow_from = $server_status->{allow_from};

        if ($allow_from->{all} || $allow_from->{$addr} || ($server_status->{authkey} && $server_status->{authkey} eq $authkey)) {
            $self->log->info("server status request from $addr - access allowed");

            if (defined $req->param("plain")) {
                $self->print_plain_server_status;
            } else {
                $self->print_json_server_status($req);
            }
        } else {
            $self->log->warning("server status request from $addr - access denied");
        }

        return 1;
    }

    return undef;
}

sub generate_server_statistics {
    my $self = shift;
    my $procs = $self->ipc->proc_status;

    my $stats = {
        procs => [], ttlreq => 0,
        S => 0, W => 0, R => 0,
        P => 0, N => 0
    };

    foreach my $proc (@$procs) {
        my $status = $proc->{status} || "F";
        $stats->{ttlreq} += $proc->{ttlreq};
        $stats->{$status}++;

        if ($proc->{pid}) {
            push @{$stats->{procs}}, $proc;
        }
    }

    return $stats;
}

sub print_plain_server_status {
    my $self = shift;
    my $stats = $self->generate_server_statistics;
    my $format = "%6s  %6s  %15s  %19s  %39s  %s\n";
    my @content;

    print "Content-Type: text/plain\n\n";

    print "* Column description\n\n";
    print "    PID     - The process id.\n";
    print "    STATUS  - The current status of the process.\n";
    print "    TTLREQ  - The total number of processed requests.\n";
    print "    TIME    - The time when the last request was processed.\n";
    print "    CLIENT  - The IP address of the client that is/were processed.\n";
    print "    REQUEST - The request of the client that is/were processed.\n\n";

    print "* Status description\n\n";
    print "    S - Starting up\n";
    print "    W - Waiting for connection\n";
    print "    R - Reading request\n";
    print "    P - Processing request\n";
    print "    N - No request received\n\n";
    print "    If the status is in RWN then the columns TIME, CLIENT and REQUEST\n";
    print "    shows information about the last request the process processed.\n\n";

    print "* Statistics\n\n";
    print "    Server time: ", $self->timestamp(time), "\n\n";
    printf "    Total requests procesesed: $stats->{ttlreq}\n\n";
    printf "%8s worker starting up\n", $stats->{S};
    printf "%8s worker waiting for incoming request\n", $stats->{W};
    printf "%8s worker reading request\n", $stats->{R};
    printf "%8s worker procesing request\n", $stats->{P};
    printf "%8s worker in status n/a\n", $stats->{N};
    printf "%8s free slots available\n\n", $stats->{F};

    print "* Process list\n\n";
    printf $format, qw(PID STATUS TTLREQ TIME CLIENT REQUEST);

    foreach my $proc (@{$stats->{procs}}) {
        printf $format,
            $proc->{pid},
            $proc->{status},
            $proc->{ttlreq},
            $self->timestamp($proc->{time}),
            $proc->{client},
            $proc->{request};
    }
}

sub print_json_server_status {
    my ($self, $req) = @_;

    my $json = defined $req->param("pretty")
        ? JSON->new->pretty(1)
        : JSON->new->pretty(0);

    print "Content-Type: application/json\n\n";
    print $json->encode({ status => "ok", data => $self->ipc->proc_status });
}

sub timestamp {
    my $self = shift;
    my $time = shift || time;
    my @time = (localtime($time))[reverse 0..5];
    $time[0] += 1900;
    $time[1] += 1;
    return sprintf "%04d-%02d-%02d %02d:%02d:%02d", @time[0..5];
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
        max_process_size => {
            type => Params::Validate::SCALAR,
            regex => qr/^(\d+\s*(M|G)B{0,1}|0)\z/i,
            default => "1GB"
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

    if ($options{max_process_size}) {
        $options{max_process_size_readable} = $options{max_process_size};
        $options{max_process_size_readable} =~ s/\s//g;
        my ($size, $unit) = ($options{max_process_size_readable} =~ /^(\d+)(M|G)B{0,1}\z/i);
        $unit = uc $unit;
        $options{max_process_size} = $unit eq "M" ? $size * 1048576 : $size * 1073741824;
    }

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
        authkey => {
            type => Params::Validate::SCALAR,
            optional => 1,
        }
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
