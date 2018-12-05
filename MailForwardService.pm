#!/usr/bin/perl

#=====================================
# SQL-Ledger
# Script for automatic customer 
# statement overview email forwarding
#
#=====================================

use SL::MailForwardService;
use Date::Calc qw(Today Week_Number);
use Cwd;
use MIME::Lite;
use Exporter;
our @ISA = ('Exporter');
our @EXPORT= qw(
&send_email
);

use Log::Log4perl;
Log::Log4perl->init('./config/fmsLogger.conf');

use Data::Dumper;

# Configurations
my $LOGS =  Log::Log4perl->get_logger ( );

sub send_email {
  my ( $From, $To, $Cc, $Sub, $Msg, $Attmnt ) = @_;

  ### Create the multipart "container":
  my $msg = MIME::Lite->new(
    From    =>"$From",
    To      =>"$To", 
    Cc      =>"$Cc",
    Subject =>"$Sub",
    Type    =>'multipart/mixed'
  );
  ### Add the text message part - BODY:
  $msg->attach(
    Type     =>'text/plain',
    Data     =>"$Msg"
  );
  ### Add message part - Attachments:
  if ($Attmnt){
    foreach (@{$Attmnt}){
      $msg->attach(
        Type 		=> 	$_->{type}, 
        Path 		=>	$_->{full_path},
        Filename 	=> 	$_->{filename},
        Disposition => 'attachment'	
      );
    }		
  }

  $LOGS->info("Sending e-mail to:".$To." Subject: ".$Sub);            
  $msg->send("sendmail") or $LOGS->error("Can't send e-mail") ;

  #TODO: remove attachements
  if ($Attmnt){
    foreach (@{$Attmnt}){
      my $rm_path = $_->{full_path};
      unlink $rm_path;
    }		
  }

}


1;
