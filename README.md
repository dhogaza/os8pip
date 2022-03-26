# os8pip utility
Provides OS/8 PIP-like functionality for OS/8 device files on Mac
OS/X.

Inspired in part by Vincent Slyngstad's Perl scripts that work
on OS/8 device files.

Written in the ISO 2011 standard version of C, verified by CLANG's
-std=c11 switch.

Common to all commands:

You must provide a path to an OS/8 device file.  Except for the create
command, this must exist and must not be currently mounted on a
PDP-8/e Simulator device.  Not sure if simh locks its mounted files.

OS/8 files must be prefixed with the string "os8:".  This allows the
program to determine what to do with your command.  It also makes the
use of file specifications like "os8:*.*" practical.  Without the "os8:"
prefix the shell might pass a list of files that match the pattern on
the host directory, probably not what you want.

File copying follows the syntax for "cp" - a string of files to
a directory, or a single file to a single file.  You can also output a
single text file to stdout.

Files recognized by extension as text files will remove or add the
mark bit (0200) and <cr> characters as needed.  Files recognized as
binary files (loader or RIM) are processed as byte streams (might
port to windows someday).  Others are processed as image files, and
copied block-by-block.

This makes it easy to copy files off of an OS/8 device file, edit it
locally in your favorite editor, then copy it back to the OS/8 device
file, mount it in the PDP-8/e Mac OS/X simulator, and build your
project.

Not that there's anything wrong with OS/8 TECO, I was one of those
who got the first version going (PS/8 TECO) and sent it to DEC...

Device files with the default extensions .rk05, .dsk, .tu56, or .dt8
are automatically recognized but can be overridden with the switches
--rk05, --dsk, --tu56, --dt8.
 
Get a directory listing of an OS/8 device file:

os8pip --os8 mytape.tu56 --dir [--empties] [--columns n]

Output an OS/8 file to stdout:
 
os8pip --os8 mydisk.rk05 os8:help.he

Copy files from an OS/8 device file to the host:

os8pip --os8 mydisk.rk05 os8:b*.* os8:pal8.pa dir_file

Copy files from the host to an OS/8 device file:

os8pip --os8 mydisk.rk05 *.pa os8:

Delete files from the OS/8 device file:

os8pip --os8 mytape.tu56 os8:b*.* os8:pal8.pa [--quiet]
 
Create a new os8 user device file (no system area), giving an
error if the file already exists:

os8pip --os8 newdisk.rk05 --create

The newly-created rk05 device file has a single file system,
rka.  To add an rkb file system:
 
os8pip --os8 newdisk.rk05 --create --rkb --exists

To get a directory of the newly created rkb file system:
 
os8pip --os8 newdisk.rk05 --dir --rkb
 
To zero an existing OS/8 device file's file system, preserving
the structure (user or system):

os8pip --os8 scratch.dsk --zero
Are you sure? y/n

Supported file formats at the moment:

 - DSK (two bytes per 12 bit-word, 512 byte blocks).
 - DECTape (two bytes per 12 bit word, 129 word blocks).
 - RK05 OS/X PDP-8/e format (3:2 packing, 384 byte blocks),
   both platters (RKAn: and RKBn: on OS/8).

 ".ba" - BASIC Source
 ".bi" - BATCH Input
 ".fc" - FOCAL Source
 ".ft" - FORTRAN Source
 ".he" - HELP
 ".hl" - HELP
 ".ls" - Listing
 ".ma" - MACRO Source
 ".pa" - PAL Source
 ".ps" - Pascal Source
 ".ra" - RALF Source
 ".ro" - Runoff Source
 ".sb" - SABR Source
 ".sl" - SABR Source
 ".te" - TECO File
 ".tx" - Text File

