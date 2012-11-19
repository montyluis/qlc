#!/usr/bin/perl -w
use utf8;
use strict;
use open qw(:std :utf8);


print "Bitte Dateiname eingeben: ";
chop(my $filename=<STDIN>);

open(FH, "<$filename")
         or
         die "$filename konnte nicht geöffnet werden\n";

#undef $/;

while(my $search=<FH>){
    chomp ($search);
            #if($search =~ m/;|,|'|-|_|"|!|¿|¡|\?|\(|\)+/gi  ) {            # sucht nach diese Bestimmte Symbole
            if($search =~ m/;|,|'|-|_|"|!|¿|¡|\?+/gi  ) {           # sucht nach diese bestimmte Symbole          
            #if($search =~ m/\w+[^aeiouáéíóúàèìòùèi̵ì̵ì]\t(http:.*)$/gi  ) {         # Wörter die nicht auf Vokalen enden
            #if($search !~ m/^\[/gi  ) {            # sucht Linien die kein [ am Anfang haben
            #if($search =~ m/\[|\]/gi  ) {          # sucht nach [ und ]
            #if($search !~ m/\[|\]/gi  ) {          # sucht nach Linien die kein [ oder ] haben
            #if($search !~ m/:\s/gi  ) {            # sucht Linien die keinen : gefolgt von einem Leerzeichen haben
            #if($search =~ m/\w+[bcdfghjklmnñpqrstvwxyz]\t(http:.*)$/gi  ) {            # sucht nach Wörter die auf Konsonanten enden
            #if($search =~ m/^.{1,2}\t(http:.*)$/gi  ) {            # sucht nach Linien die maximal 2 Charaktere am Anfang vor dem Link haben
            #if($search =~ m/^[a-záéíóúàèìòùèi̵ì̵ì]{1} /gi  ) {                     # sucht nach Wörter am Anfang der Linie mit einer Einzelne Buchstabe
            #if($search =~ m/\s[a-záéíóúàèìòùèi̵ì̵ì]{1}\t(http:.*)$/gi  ) {         # sucht nach einer Einzelne Buchstabe nach einem Leerzeichen und vor dem Link
            #if($search =~ m/\] .+\.(.*)\t(http:.*)$/gi  ) {            # sucht nach ] gefolgt von einem Leerzeichen und Wörter mit einem Punkt
            #if($search =~ m/^[^\[].*(.)+; (.)+[^\]]/gi) {          # sucht nach ; die sich zwischen keine [ ] befinden
            #if($search =~ m/(.)+; (.)+/gi && $search !~ m/\[(.)*; (.)*(\])?/gi && $search !~ m/^\[(.)*; (.)*/gi) {         # sucht nach ; die sich zwischen keine [ ] befinden
            #if($search =~ m/(.)*\/(.)*\t(http:.*)$/gi  ) {         # sucht nach / vor dem Link
            #if($search =~ m/(-)+/gi  ) {           # sucht nach mindesten einen -
            #if($search =~ m/(-)+(.)+(-)+/gi  ) {           # sucht nach mindestens einen Symbol zwischen -
                   print "$search\n";
                  }
      }
close FH