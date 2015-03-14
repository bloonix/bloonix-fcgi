package Bloonix::FCGI;

use strict;
use warnings;
use Bloonix::FCGI::Request;
use IO::Socket;
use FCGI;
use Params::Validate qw();
use POSIX qw(:sys_wait_h);
use Log::Handler;
use Time::HiRes qw();

use constant PARENT_PID => $$;

use base qw(Bloonix::Accessor);
__PACKAGE__->mk_accessors(qw/log proc request socket/);

our $VERSION = "0.7";

sub new {
    my $class = shift;
    my $opts = $class->validate(@_);
    my $self = bless $opts, $class;
    $self->init;
    return $self;
}

sub init {
    my $self = shift;

    $self->log(Log::Handler->get_logger("bloonix"));
    $self->init_fcgi_socket;
    $self->init_fcgi_request;
}

sub init_fcgi_socket {
    my $self = shift;

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
}

sub init_fcgi_request {
    my $self = shift;

    $self->{request} = FCGI::Request(
        \*STDIN,
        \*STDOUT,
        \*STDERR,
        \%ENV,
        $self->socket,
        &FCGI::FAIL_ACCEPT_ON_INTR
    );
}

sub accept {
    my $self = shift;

    return $self->request->Accept() >= 0;
}

sub get_new_cgi {
    my $self = shift;

    return Bloonix::FCGI::Request->new();
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
        port => {
            type => Params::Validate::SCALAR,
            default => 9000
        },
        listen => {
            type => Params::Validate::SCALAR,
            default => SOMAXCONN # from IO::Socket
        },
        lockfile => {
            type => Params::Validate::SCALAR,
            optional => 1
        }
    });

    return \%options;
}

sub DESTROY {
    if ($$ == PARENT_PID) {
        FCGI::CloseSocket(shift->{socket});
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

Copyright (C) 2009 by Jonny Schulz. All rights reserved.

=cut
