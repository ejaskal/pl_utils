######################################################################
# FMS 
# Copyright (C) 2016
#
#  Author: Maurice Ward & Co.
#     Web: http://www.mauriceward.com
#
#######################################################################
#
# FMS DB service routines
#
#######################################################################

package SL::DBService;

use warnings;
use strict;
use DBI;
use Data::Dumper;

use Exporter;
our @ISA = ('Exporter');
our @EXPORT= qw(
  &db_connect
  &get_datasets
  &select_all_to_array 
  &select_cols_to_array 
  &select_count
  &update_table
  &table_exists
  );


use Log::Log4perl;
Log::Log4perl->init('./config/fmsLogger.conf');
my $LOGS = Log::Log4perl->get_logger();

sub db_connect {
    my ( $conf, $dset ) = @_;
    $LOGS->info("Log in database.....");

    $dset = $dset || 'template1';
    $LOGS->info("Create DB connection to dataset: ".$dset);

    my $dbh = DBI->connect(
        $conf->{DB_CONNECTOR}.''.$dset,
        $conf->{DB_LOGIN},
        $conf->{DB_PASSWORD}
    ) || $LOGS->error( "Unable create connection to database $dset...\n ".$DBI::errstr ) ;

    return $dbh;
}


sub get_datasets {
  my ( $dbh, $conf) = @_;

  $LOGS->debug('Getting list of available datasets....');
  my $query = $dbh->prepare("SELECT datname FROM pg_database");
  $query->execute();
  my $db;
  $query->bind_columns( \$db );
  my $databases = ();
  while ( $query->fetch ) {
    push( @$databases, $db ) if ( $db !~ /$conf->{DB_ESCAPE}/);
  }
  return $databases;
}


sub select_all_to_array {
    my ( $dbh, $table, $condition, $inOrder ) = @_;

    my $where = ( $condition ? " WHERE $condition " : '' );
    my $order = ( $inOrder ? " ORDER BY $inOrder " : '' );

  my @data;

    my $query = qq|
    SELECT * FROM $table |;
    $query .= $where;
    $query .= $order;
    my $sth = $dbh -> prepare ( $query );
    $sth -> execute || $LOGS->error( "Unable finish request: ".$sth->errstr."\n\nQUERY: ".$query );
    

    while ( my $ref = $sth -> fetchrow_hashref("NAME_lc") ){
        push ( @data, $ref );
    }

    return \@data;
}


sub select_cols_to_array {
  my ( $dbh, $items, $table, $condition, $sort ) = @_;

  my $cols = ( $items ? join (',', @{$items} ) :  '*' );
  my $where = ( $condition ? "WHERE $condition" : '' );
  my $order = ( $sort ? "ORDER BY $sort" : '' );

  my @data;

  my $query = qq|
    SELECT $cols
    FROM $table
    $where
    $order
  |; 

  my $sth = $dbh->prepare($query);
  $sth->execute() || $LOGS->error( "Unable finish request: ".$sth->errstr."\n\nQUERY: ".$query );

  while ( my $ref = $sth->fetchrow_hashref("NAME_lc") ){
    push (@data, $ref);  
  }

  return \@data;
}


sub select_count {
  my ( $dbh, $table, $condition ) = @_;

  my $where = ( $condition ? "WHERE $condition" : '' );

  my @data;

  my $query = qq|
    SELECT COUNT (*) 
    FROM $table
    $where
  |; 

  my $sth = $dbh->prepare($query);
  $sth->execute() || $LOGS->error( "Unable finish request: ".$sth->errstr."\n\nQUERY: ".$query );

  my ($count) = $sth->fetchrow_array;

  return $count;
}

sub update_table {
    my ( $dbh, $table, $keys, $vals, $where ) = @_;

    my $query = qq| UPDATE $table SET |.join( ' ,', map ( $_.'=?' , @{$keys} ) );
    $query .= $where;

    my $sth = $dbh -> prepare ( $query );
    $sth -> execute ( @{$vals} ) || $LOGS->error("Unable finish request: ".$sth->errstr."\n\nQUERY: ".$query );
}


sub table_exists {
  my ($dbh, $table_name) = @_;
  my $sth = $dbh->table_info(undef,'public', $table_name, 'TABLE');
  
  $sth->execute;

  my @info = $sth->fetchrow_array;
  my $exists = scalar @info;

  return $exists;
}


1;
