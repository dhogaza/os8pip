/* 
   Provides much of the functionality of OS/8's PIP program, directly on
   OS/8 device image files.

   Copyright (c) 2022 Don R Baccus

   This program is free software: you can redistribute it and/or modify  
   it under the terms of the GNU General Public License as published by  
   the Free Software Foundation, version 3.
  
   This program is distributed in the hope that it will be useful, but 
   WITHOUT ANY WARRANTY; without even the implied warranty of 
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU 
   General Public License for more details.
  
   You should have received a copy of the GNU General Public License 
   along with this program. If not, see <http://www.gnu.org/licenses/>.

*/

#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <ctype.h>
#include <limits.h>
#include <assert.h>
#include <libgen.h>
#include <sys/file.h>

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

typedef unsigned short pdp8_word_t;
typedef char * str_t;
typedef const char * const_str_t;

#define OS8_BLOCK_SIZE 256

/* The following DECTape constants are in bytes */
#define DECTAPE_BLOCK_SIZE 258
#define DECTAPE_LENGTH 380292
#define OS8_DECTAPE_LENGTH 377344
#define DECTAPE_BLOCKS OS8_DECTAPE_LENGTH / (OS8_BLOCK_SIZE * 2)

/* Mac PDP-8/e simulator packs two 12-bit words in three bytes */
const unsigned RK05_BLOCK_SIZE = 384; /* 256 words */
const unsigned RK05_RKB_OFFSET = 3248; /* blocks */

#define DIR_LENGTH 6
const unsigned FIRST_DIR_BLOCK=1;

typedef struct {
    unsigned last_block_no;
    unsigned filesystem_size;
    unsigned size;
} device_t;

/* Byte buffer must be arge enough to hold 2 129-word DECTape blocks.  This
   is large enough to hold an RK05 block with two words backed into three
   bytes, as well as n Simh block in dsk format
*/
typedef unsigned char byte_buffer_t[OS8_BLOCK_SIZE * 2 + 2];
typedef pdp8_word_t os8_block_t[OS8_BLOCK_SIZE];

/* OS/8 directory structure */

typedef struct {
    pdp8_word_t number_files; /* negative 12 bits */
    pdp8_word_t first_file_block;
    pdp8_word_t next_segment; /* device block number, 0 flags last segment */
    pdp8_word_t flag_word; /* 0 no tentative entry, 01400-01777 otherwise */
    pdp8_word_t additional_words; /* negative 12 bits, usually -1 i.e. date word */
    pdp8_word_t file_entries[];
} dir_struct_t;

typedef struct {
    bool dirty; /* if we modify the directory segment we need to write it out */
    union {
        dir_struct_t dir_struct;
        os8_block_t data;
    } d;
} dir_block_t;

/* An entire OS/8 directory is short and sequentially allocated so we'll just hold the
   whole thing in memory.
*/

typedef dir_block_t directory_t[DIR_LENGTH];

typedef struct {
    pdp8_word_t file_number;
    pdp8_word_t next_block;
    pdp8_word_t *entry;
    dir_block_t *dir;
    dir_block_t *dir_block;
} cursor_t;

/* sixbit name representation */
typedef pdp8_word_t name_t[4];
/* up to a six character filename part, ".", and a two character
   extension, and the null terminator.
*/
typedef char os8_filename_t[10];

typedef struct {
    bool empty_file;
    name_t name;   /* First word zero flags empty file */
    dir_block_t *dir_block; 
    pdp8_word_t *entry;
    pdp8_word_t file_block;
    pdp8_word_t file_number;
    pdp8_word_t length;
    pdp8_word_t additional_words[10]; /* should be plenty, usually just one */
    pdp8_word_t additional_count;
} entry_t;

const unsigned empty_entry_length = 2; /* flag + length words */

typedef enum {unknown, dectape, dsk, rk05, tu56} format_t;
typedef enum {base, rka, rkb} rk05_filesystem_t; /* currently RK05 RKA and RKB only */

/* Used for wildcard matching of filenames in sixbit */
typedef struct {
    name_t mask;
    name_t match;
} pattern_t;

void dump_bytes(byte_buffer_t byte_buffer, unsigned length)
{
    unsigned i = 0;
    while (i < length) {
        if (i % 16 == 0) {
            printf("%06o  ", i);
        }
        printf(" %03o", (unsigned)byte_buffer[i]);
        if (++i % 16 == 0) {
            printf("\n");
        }
    }
    putchar('\n');
}

void dump_words(os8_block_t block_buffer)
{
    unsigned i = 0;
    while (i < OS8_BLOCK_SIZE) {
        if (i % 8 == 0) {
            printf("%06o  ", i*2);
        }
        printf(" %06o", block_buffer[i]);
        if (++i % 8 == 0) {
            printf("\n");
        }
    }
    putchar('\n');
}

void dump_entry(entry_t entry)
{
    printf("file block: %2d ", entry.file_block);
    printf("dir_block: %p ", (void *)entry.dir_block);
    printf("entry: %p ", (void *)entry.entry);
    printf("empty_file: %d\n", entry.empty_file);
    if (!entry.empty_file) {
        int i;
        for (i = 0; i < 4; i++) {
            printf("name[%d]: %6o ", i, entry.name[i]);
        }
        printf("additional count: %d ", entry.additional_count);
        for (i = 0; i < MIN(10, entry.additional_count); i++) {
            printf("additiona_words[%d]: %6o\n", i, entry.additional_words[i]);
        }
    }
    printf("length: %d\n", entry.length);
}

/* Miscellaneous simple and useful stuff */

/* perform a twos-complement negation */
int negate(pdp8_word_t word) {
    return (4096 - word) % 4096;
}

bool yes_no(const_str_t prompt)
{
    char ch;
    printf("%s ", prompt);
    scanf(" %c", &ch);
    return ch == 'y' || ch == 'Y'; 
}

bool yes_no_sure()
{
    return yes_no("Are you sure? ");
}

void shuffle_words_up(pdp8_word_t *src, pdp8_word_t *dst, pdp8_word_t *last)
{
    while (src >= last) {
        *dst-- = *src--;
    }
}

void shuffle_words_down(pdp8_word_t *src, pdp8_word_t *dst, pdp8_word_t *last)
{
    while (src <= last) {
        *dst++ = *src++;
    }
}

/* these convert in place and have all the limitations of tolower/upper so be careful */

void convert_lower(char *s)
{
    for (; *s != '\0'; s++) {
        *s = tolower(*s);
    }
}

void convert_upper(char *s)
{
    for (; *s != '\0'; s++) {
        *s = toupper(*s);
    }
}

/* Filename handling stuff */

const_str_t os8_prefix = "os8:";

typedef enum {text_type, binary_type, unknown_type} filename_type_t;

filename_type_t filename_type(char *filename)
{
    static char* text_extensions[] = {
        ".ba", /* BASIC Source */
        ".bi", /* BATCH Input */
        ".fc", /* FOCAL Source */
        ".ft", /* FORTRAN Source */
        ".he", /* HELP */
        ".hl", /* HELP */
        ".ls", /* Listing */
        ".ma", /* MACRO Source */
        ".pa", /* PAL Source */
        ".ps", /* Pascal Source */
        ".ra", /* RALF Source */
        ".ro", /* Runoff Source */
        ".sb", /* SABR Source */
        ".sl", /* SABR Source */
        ".te", /* TECO File */
        ".tx", /* Text File */
        NULL
    };

    char *dot_pos;

    if ((dot_pos = strrchr(filename, '.')) == NULL) {
        return unknown_type;
    }

    char extension[strlen(dot_pos)];
    strcpy(extension, dot_pos);
    convert_lower(extension);
    for (char **p = text_extensions; *p != NULL; p++) {
        if (strcmp(extension, *p) == 0) {
            return text_type;
        }
    }
    return unknown_type;
}

bool os8_filename_part_p(char *part, unsigned length)
{
    for (char *s = part; s < part + length; s++) {
        if (*s == '*') {
            return s == part + length - 1;
        }
        if (!isalnum(*s) || (s == part && !isalpha(*s))) {
            return false;
        }
    }
    return true;
}

bool os8_filename_p(char *filename)
{
    /* A legal os8 file namespec looks like "filename[.extension]". The file name is
       restricted to six alphanumeric characters with an optional single trailing
       "*" wildcard character included, or an extension of up to two alphanumeric characters
       with a single optional trailing "*" included.  Upper and lower case characters are
       accepted with the understanding that when the file is read or written from/to OS/8
       it will be converted to upper case.

       Unix shells, at least, glob all files on the command line so no wildcards will be
       seen.
    */

    int file_length = strcspn(filename, ".");
    int extension_length = strlen(filename) - file_length;

    /* extension length includes "." if it was found */
    if (extension_length != 0 && (extension_length > 3 ||
        !os8_filename_part_p(filename + file_length + 1, extension_length - 1))) {
        return false;
    } 
    if (file_length < 1 || file_length > 6 ||
        !os8_filename_part_p(filename, file_length)) {
        return false;
    }
    return true;
}

bool os8_file_spec_p(char *spec)
{
    /* A legal os8 file spec is a legal OS/8 file name with the prefix "os8:".  Requiring
       the prefix prevents shell globbing from messing stuff up unless you have a local
       directory named "os8:" (just don't).
    */
    return strncmp(spec, os8_prefix, strlen(os8_prefix)) == 0 &&
           os8_filename_p(spec + 4);
}

bool os8_contains_wildcard_p(char *filename)
{
    return strchr(filename, '*') != NULL;
}

bool os8_devicename_p(char *devicename)
{
    return strcmp(devicename, os8_prefix) == 0;
}

/* Verify the validity of the filename before calling these routines. */
const_str_t strip_device(const_str_t filename)
{
    return strstr(filename, "os8:") ? filename + 4 : filename;;
}

void build_sixbit(const_str_t filename, name_t name)
{

    name[0] = 0; name[1] = 0; name[2] = 0; name[3] = 0;

    unsigned i = 0;

    for (const_str_t s = filename; *s; s++) {
        if (*s == '*') {
            continue;
        } 
        if (*s == '.') {
            i = 6;
            continue;
        }
        /* handle all the cases! */

        char c = tolower(*s);
        c = c < (char) 0140 ? c : c - (char) 0140;
        name[i/2] = i & 1 ? (pdp8_word_t)(c) | name[i/2] : (pdp8_word_t)(c) << 6;
        i++;
    }
}

void build_mask_part(const_str_t filename, name_t mask, unsigned start, unsigned finish)
{
    unsigned i = start;
    for (const_str_t s = filename; *s; s++) {
        if (*s == '.') {
           return;
        }
        if (*s == '*') {
            while (i <= finish) {
                mask[i/2] &= i & 1 ? 07700 : 0;
                i++;
            }
            return;
        } 
        i++;
    }
}

void build_mask(const_str_t filename, name_t mask)
{

    char *dot_pos;
    mask[0] = 07777; mask[1] = 07777; mask[2] = 07777; mask[3] = 07777;

    build_mask_part(filename, mask, 0, 5);
    if ((dot_pos = strchr(filename, '.')) != NULL) {
        build_mask_part(dot_pos + 1, mask, 6, 7);
    }
}

void build_pattern(const_str_t filename, pattern_t *pattern)
{
    build_sixbit(filename, pattern->match);
    build_mask(filename, pattern->mask);
}

bool pattern_match_p(name_t name, pattern_t pattern)
{
    return !((name[0] ^ pattern.match[0]) & pattern.mask[0] ||
             (name[1] ^ pattern.match[1]) & pattern.mask[1] ||
             (name[2] ^ pattern.match[2]) & pattern.mask[2] ||
             (name[3] ^ pattern.match[3]) & pattern.mask[3]);
}

char *cvt_from_sixbit(pdp8_word_t sixbit, char * filename)
{
    unsigned byte1 = sixbit >> 6;
    unsigned byte2 = sixbit & 077;
    if (byte1 != 0) {
        *filename++ = byte1 < 040 ? (char)(byte1 + 0140) : (char)byte1;
    }
    if (byte2 != 0) {
        *filename++ = byte2 < 040 ? (char)(byte2 + 0140) : (char)byte2;
    } 
    return filename;
}

/* filename buffer must be at least 10 characters */
void get_filename(name_t name, char *filename)
{

    for (int i = 0; i < 3; i++) {
        filename = cvt_from_sixbit(name[i], filename);
    }
    if (name[3]) {
        *filename++ = '.'; 
        filename = cvt_from_sixbit(name[3], filename);
    }
    *filename = '\0';
}

/* OS/8 Directory handling code */

unsigned index_from_dir_block(directory_t directory, dir_block_t *dir_block)
{
    unsigned segment = 0;
    while (&directory[segment] != dir_block) {
        segment += 1;
        assert(segment != DIR_LENGTH);
    } 
    return segment;
}

/*
  I learned the hard way it is easy to forget to set dirty true.
*/

void bump_number_files(dir_block_t *dir_block, int amount)
{
    dir_block->dirty = true;
    dir_block->d.dir_struct.number_files -= amount;
}

void bump_first_file_block(dir_block_t *dir_block, int amount)
{
    dir_block->dirty = true;
    dir_block->d.dir_struct.first_file_block += amount;
}

void set_next_segment(dir_block_t *dir_block, int next_segment)
{
    dir_block->dirty = true;
    dir_block->d.dir_struct.next_segment = next_segment;
}

void init_cursor(directory_t directory, cursor_t *cursor)
{
    cursor->dir = directory;
    cursor->dir_block = directory;
    cursor->entry = cursor->dir_block->d.dir_struct.file_entries; 
    cursor->next_block = cursor->dir_block->d.dir_struct.first_file_block;
    cursor->file_number = 1;
}

/* need to store additional words in the file entry rather than just point
   to them!
*/

bool overflowed_segment(cursor_t cursor)
{
   return cursor.file_number > negate(cursor.dir_block->d.dir_struct.number_files);
}

/* set cursor to the next valid file entry */
bool valid_entry(cursor_t *cursor)
{
    while (overflowed_segment(*cursor)) {
        int next_segment = cursor->dir_block->d.dir_struct.next_segment;
        if (next_segment == 0) {
            return false; /* we've read the entire directory */
        }
        assert(next_segment <= DIR_LENGTH);
        cursor->file_number = 1;
        cursor->dir_block = &(cursor->dir[next_segment - 1]);
        cursor->next_block = cursor->dir_block->d.dir_struct.first_file_block;
        cursor->entry = cursor->dir_block->d.dir_struct.file_entries; 
    }
    return true;
}

unsigned file_entry_length(dir_struct_t dir_struct)
{
    return sizeof(name_t) / sizeof(pdp8_word_t) + 1 +
           negate(dir_struct.additional_words);
}

unsigned entry_length(entry_t entry)
{
    return entry.empty_file ? empty_entry_length :
                              file_entry_length(entry.dir_block->d.dir_struct);
}

/* Move cursor to the next file in the a directory segment */
void advance_cursor(cursor_t *cursor, entry_t entry)
{
    cursor->file_number++;
    cursor->entry += entry_length(entry);
    cursor->next_block += entry.length;
}    

/* this can be used by functions like delete file that peek
   ahead to see if the file to be deleted is followed by an
   empty file. You have to initalize the cursor first if you
   care about the dir field.
*/
void restore_cursor(cursor_t *cursor, entry_t entry)
{
    cursor->file_number = entry.file_number;
    cursor->entry = entry.entry;
    cursor->next_block = entry.file_block;
    cursor->dir_block = entry.dir_block;
}    

/* get file entry data but don't advance cursor */
void peek_entry(cursor_t cursor, entry_t *entry)
{
    entry->file_block = cursor.next_block;
    entry->dir_block = cursor.dir_block;
    entry->entry = cursor.entry;
    entry->file_number = cursor.file_number;
    if (*cursor.entry == 0) { /*empty file */
        entry->empty_file = true;
        cursor.entry++;
    } else {
        entry->empty_file = false;
        int i = 0;
        for (i = 0; i < 4; i++) {
            entry->name[i] = *cursor.entry++;
        }
        entry->additional_count = negate(cursor.dir_block->d.dir_struct.additional_words);
        for (i = 0; i < MIN(10, entry->additional_count); i++) {
            entry->additional_words[i] = cursor.entry[i];
        }
        cursor.entry += entry->additional_count;
    }
    entry->length = negate(*cursor.entry);
}    

/* get file entry data and advance cursor */
void get_entry(cursor_t *cursor, entry_t *entry)
{
    peek_entry(*cursor, entry);
    advance_cursor(cursor, *entry);
}    

/* puts file entry data and marks the current directory block dirty */
void put_entry(entry_t entry)
{
    entry.dir_block->dirty = true;
    if (entry.empty_file) {
        *entry.entry++ = 0;
    } else {
        int i = 0;
        for (i = 0; i < 4; i++) {
            *entry.entry++ = entry.name[i];
        }
        for (i = 0; i < MIN(10, entry.additional_count); i++) {
            entry.entry[i] = entry.additional_words[i];
        }
        entry.entry += entry.additional_count;
    }
    *entry.entry = negate(entry.length);
}    

/* We try to maintain the flag word if possible.  Someone might have ENTERed
   a file without CLOSEing it, unmounted it, is writing a file here, and
   expects to be able to CLOSE it if they remount it on the same device.  I
   don't know why anyone would expect that to work.  But it might. Except
   when you write a file using this program and it bumps your tenative entry
   to the next page.  Then we give up.
*/
void fix_segment_up(entry_t entry, unsigned offset, pdp8_word_t *first_byte)
{
    shuffle_words_up(first_byte, first_byte + offset, entry.entry);
    pdp8_word_t *flag_word = &(entry.dir_block->d.dir_struct.flag_word);
    if (*flag_word != 0 &&
        (entry.dir_block->d.data + (*flag_word - 01400)) > entry.entry) {
        *flag_word = *flag_word + offset > 01777 ? 0 : *flag_word + offset;;
    }
}

void fix_segment_down(entry_t entry, unsigned offset)
{
    shuffle_words_down(entry.entry + entry_length(entry), entry.entry + offset,
                  &entry.dir_block->d.data[OS8_BLOCK_SIZE - 1]);
    pdp8_word_t *flag_word = &(entry.dir_block->d.dir_struct.flag_word);
    if (*flag_word != 0 &&
        (entry.dir_block->d.data + (*flag_word - 01400)) > entry.entry) {
        *flag_word -= entry_length(entry) - offset;
    }
}

bool validate_directory(directory_t directory)
{
    /* 
       Does some sanity checking on a directory structures.
       Needs to do much more after ...
    */
    int i = 0;
    do {
        if (!(directory[i].d.dir_struct.next_segment <= 6 &&
              directory[i].d.dir_struct.number_files != 0 &&
              negate(directory[i].d.dir_struct.number_files) < 100 &&
              negate(directory[i].d.dir_struct.additional_words) < 10 &&
              (directory[i].d.dir_struct.flag_word == 0 ||
               directory->d.dir_struct.flag_word >= 01400 && directory[i].d.dir_struct.flag_word <= 01777))) {
            return false;
        }
        i = directory[i].d.dir_struct.next_segment - 1;
    } while (i >= 0);
    return true;
}

void consolidate(directory_t directory)
/*
    Sweep through directory segments repeatedly consolidating two empty
    files next to each other into a single one.

    Unlike the CONSOL routine of OS/8's USR (found in OS8.PA), we do this in
    one pass and do it to all of the segments rather than just one.

    Just like the CONSOL routine we do each segment individually, which
    can leave and empty entry at the end of one segment abutting an empty
    entry at the beginning of the next block.

    We consolidate two empty files that are adjacent and also zero-length
    empty files that aren't the only file in their segment (segments with
    zero files break things, here and in OS/8).

    Deleting abutting empty files on adjacent segments would be easy if
    the following empty file isn't the only file in the segment.  After
    combining them, the file count  stored in the following segment would
    have to be bumped (it is stored as a negative number), and the first file
    block for all following segments adjusted by the amount with the
    empty file.  If it were the only file all of the segments following
    would have to be shuffled down and their next segment values adjusted
    if not zero.

    In theory just unlinking the zeroed segment from the segment list would
    work as the USR follows the next segment links.  However, when the USR
    does an ENTER on the directory and is forced to add a new segment, it
    always does so at the end so a hole would be lost.

    For now I am most comfortable returning the same result that the USR
    does with its CONSOL routine. Eventually my perfectionist side will
    probably overcome this, though.

*/
{
    cursor_t cursor;
    entry_t entry;
    entry_t next_entry;

    init_cursor(directory, &cursor);
    while (valid_entry(&cursor)) {
        get_entry(&cursor, &entry);
        peek_entry(cursor, &next_entry);
        if (entry.empty_file) {
            if (entry.length == 0) {
                /* remove zero length empty file */
                fix_segment_down(entry, 0);
                bump_number_files(entry.dir_block, -1);
            } else if (!overflowed_segment(cursor) && next_entry.empty_file) {
                /* we have found two adjacent empty entries in the same
                   segment.
                */
                entry.length += next_entry.length;
                put_entry(entry);

                /* now scrunch the segment on top of the second empty file */
                fix_segment_down(next_entry, 0);
                bump_number_files(entry.dir_block, -1);
                restore_cursor(&cursor, entry); /* let's look at our empty file again */
            }
        }
    }
}

/*
   Just like the OS/8's USR , we do this by walking the entries until we find the last.
*/
void get_last_entry(dir_block_t *dir_block, entry_t *entry)
{
    cursor_t cursor;

    cursor.dir = NULL;
    cursor.dir_block = dir_block;
    cursor.entry = dir_block->d.dir_struct.file_entries; 
    cursor.next_block = dir_block->d.dir_struct.first_file_block;
    cursor.file_number = 1;

    while (!overflowed_segment(cursor)) {
        get_entry(&cursor, entry);
    }
}

/* If there's enough space in the segment for a new entry of the given size,
   return a pointer to the word within the segment, NULL otherwise.
*/
pdp8_word_t *get_unused_ptr(dir_block_t *dir_block, unsigned size)
{
    entry_t entry;

    get_last_entry(dir_block, &entry);

    /* We're at the end, return a pointer to a new entry if there's room */
    pdp8_word_t *empty_ptr = entry.entry + entry_length(entry);
    return empty_ptr + size < &(entry.dir_block->d.data[OS8_BLOCK_SIZE]) ?
           empty_ptr : NULL;
}

/* Like OS/8's USR MENTER routine, if size is zero it returns the biggest empty
   file available.  If not it returns the empty file that best fits the requested
   size.  Unlike the USR the request size isn't restricted to 255 bytes. The
   exclude entry is provided to avoid grabbing a file we've just deleted, and
   thereby overwriting its data blocks.
*/

bool get_empty_entry(directory_t directory, entry_t exclude_entry,
                     entry_t *best_entry, unsigned length)
{
    entry_t entry;
    cursor_t cursor;

    best_entry->length = 0;

    init_cursor(directory, &cursor);
    while (valid_entry(&cursor)) {
        get_entry(&cursor, &entry);
        if ((entry.dir_block != exclude_entry.dir_block ||
             entry.file_number != exclude_entry.file_number) &&
            entry.empty_file && entry.length >= length &&
           (best_entry->length == 0 ||
            (length == 0 ? entry.length > best_entry->length :
                           entry.length < best_entry->length))) {
            *best_entry = entry;
        }
    }
    return best_entry->length != 0;
}

/* Look up the next matching file from a directory and a cursor.  Intialize
   the cursor before calling the first time.
*/

bool lookup(const_str_t filename, directory_t directory, cursor_t *cursor,
            entry_t *entry)
{
    pattern_t pattern;
    build_pattern(strip_device(filename), &pattern);

    while (valid_entry(cursor)) {
        entry_t local_entry;
        get_entry(cursor, &local_entry);
        if (!local_entry.empty_file && local_entry.length != 0 &&
            pattern_match_p(local_entry.name, pattern)) {
                *entry = local_entry;
                return true;
        }
    }
    return false;
}

/* Enter a file into the directory after the data has been written.

   This is a bit different than how it works in OS/8.

   The caller should first call get_empty_entry with the required size,
   if it is known.  It is then safe to write that many blocks.

   Then pass the file entry structure for the empty file that has been
   replaced along with new filename and the actual length of the file
   that has been written.

   The caller is responsible for not writing more data than is available
   in the empty file.
*/

bool enter(const_str_t filename, const int length, directory_t directory, entry_t entry)
{
    unsigned old_len;
    unsigned len;
    cursor_t cursor;
    pdp8_word_t *unused_ptr;

    unsigned new_entry_length = file_entry_length(entry.dir_block->d.dir_struct);

    /*
       Testing shows that OS/8's USR MENTER routine doesn't entirely fill up a
       segment so we won't either, as doing so might break the real thing.
    */
    unsigned min_free_length = new_entry_length + empty_entry_length;

    while ((unused_ptr = get_unused_ptr(entry.dir_block, min_free_length)) == NULL) {
        /*
           No room in the segment that the entry lives in.  So we need to start
           shuffling entries from the end of one segment to the beginning of the
           next, iteratively making room until we can finally add our new file 
           information in front of the empty entry we are given.

           This isn't the most efficent way go to about this but at least we're not
           shuffling everything on a disk like OS/8 does, or worse, a DECTape!
        */
        dir_block_t *dir_block = entry.dir_block;

        /* try to find a segment that can take one entry from the end of the
           previous segment, starting with the segment the entry is on.
        */
        unsigned next_segment;
        while ((next_segment = dir_block->d.dir_struct.next_segment) != 0) {
            pdp8_word_t *next_unused_ptr;
            dir_block_t *next_dir_block = &directory[next_segment - 1];

            if ((next_unused_ptr = get_unused_ptr(next_dir_block, min_free_length)) != NULL) {
                entry_t last_entry;
                get_last_entry(dir_block, &last_entry);

                /* our best sized entry might be the last entry in the segment, oh my! */
                bool move_entry = last_entry.file_number == entry.file_number &&
                                  last_entry.dir_block == entry.dir_block;

                /* dir_block's loss is next_dir_block's gain */
                bump_number_files(dir_block, -1);
                bump_number_files(next_dir_block, 1);
                bump_first_file_block(next_dir_block, -last_entry.length);

                /* make the last entry on this dir_block become the first on the next */
                last_entry.dir_block = next_dir_block;
                last_entry.entry = next_dir_block->d.dir_struct.file_entries;
                last_entry.file_number = 1;
                last_entry.file_block = next_dir_block->d.dir_struct.first_file_block;

                fix_segment_up(last_entry, entry_length(last_entry), next_unused_ptr);

                /* store the entry in the new segment */
                put_entry(last_entry);

                if (move_entry) {
                    entry = last_entry;
                }
                break;
            }
            dir_block = next_dir_block;
        }

        /* If next_segment is zero, there is absolutely no room in the existing segments
           so we need to add one if possible.  When allocating, OS/8 assumes there are no
           holes in the list of segments even though it is kept in linked-list form, so
           we'll do the same.
        */
        if (dir_block->d.dir_struct.next_segment == 0) {
            pdp8_word_t index;
            /* we could probably add bookkeeping above to avoid computing first file block */
            entry_t temp_entry;
            if ((index = index_from_dir_block(directory, dir_block) + 1) < DIR_LENGTH) {
                set_next_segment(dir_block, index + 1);
                directory[index].d.dir_struct.number_files = negate(1);
                get_last_entry(dir_block, &temp_entry);
                directory[index].d.dir_struct.first_file_block = temp_entry.file_block +
                                                    entry_length(temp_entry);
                directory[index].d.dir_struct.next_segment = 0;
                directory[index].d.dir_struct.flag_word = 0;
                directory[index].d.dir_struct.additional_words = dir_block->d.dir_struct.additional_words;

                /* this zero-length entry file will be removed by consolidate */
                directory[index].d.dir_struct.file_entries[0] = 0;
                directory[index].d.dir_struct.file_entries[1] = 0;

            } else {
                return false;
            }
        }
    }

    assert(validate_directory(directory)); /* after all that, wouldn't you? */

    /* must be done first thing */
    fix_segment_up(entry, new_entry_length, unused_ptr);

    bump_number_files(entry.dir_block, 1);

    entry.empty_file = false;
    build_sixbit(filename, entry.name); 
    entry.additional_count = negate(entry.dir_block->d.dir_struct.additional_words);
    for (int i = 0; i < MIN(10, entry.additional_count); i++) {
        entry.additional_words[i] = 0;
    }
    entry.length = length;
    put_entry(entry);

    restore_cursor(&cursor, entry);
    advance_cursor(&cursor, entry);
    peek_entry(cursor, &entry);

    /* if we fail these assertionthe caller probably passed us a bogus entry
       rather than the empty file we gave them earlier.
    */
    assert(entry.empty_file);
    assert(entry.length >= length);

    entry.length -= length;
    /* write over old empty file to save its diminished length */
    put_entry(entry);

//    if ((entry.length -= length) == 0) {
//        /* wipe out our empty file of length zero */
//        fix_segment_down(entry, 0);
//        bump_number_files(entry.dir_block, -1);
//    } else {
//        /* write over old empty file to save its diminished length */
//        put_entry(entry);
//    }
    consolidate(directory);
    return true;
}

/* End of directory manipulation */

/* readers and writers are assigned when the device image file type is determined*/

typedef bool (*block_io_t)(int, unsigned, os8_block_t);

bool byte_buffer_to_word_buffer(unsigned block_no, byte_buffer_t byte_buffer, os8_block_t block_buffer)
{
    unsigned char *byte_ptr = byte_buffer;
    pdp8_word_t *word_ptr = block_buffer;

    do {
        /* CLANG doesn't like two autoincrements on both sides of an "|" or other
           commutative operators because the optimizer might flip the order of execution.
        */
        *word_ptr =  *byte_ptr++;
        *word_ptr |= *byte_ptr++ << 8;
        if ((*word_ptr++ & 0170000) != 0) {
            printf("block %i appears to be corrupted\n", block_no);
            return false;
        }
    } while (word_ptr < block_buffer + OS8_BLOCK_SIZE);
    return true;
}

bool write_dsk_block(int os8_file, unsigned block_no, os8_block_t block_buffer)
{
    byte_buffer_t byte_buffer;

    unsigned char *byte_ptr = byte_buffer;
    pdp8_word_t *word_ptr = block_buffer;   

    do {
        if ((*word_ptr & 0170000) != 0) {
            printf("Buffer for block %i appears to be corrupted, write aborted\n", block_no);
            return false;
        }
        *byte_ptr++ = *word_ptr & 0377;
        *byte_ptr++ = *word_ptr++ >> 8;
    } while (word_ptr < block_buffer + OS8_BLOCK_SIZE);

    unsigned bytes = pwrite(os8_file, byte_buffer, OS8_BLOCK_SIZE * 2, block_no * OS8_BLOCK_SIZE * 2);
    return bytes == OS8_BLOCK_SIZE * 2;
}

bool write_dectape_block(int os8_file, unsigned block_no, os8_block_t block_buffer)
{
    byte_buffer_t byte_buffer;

    /* Unconverted simh DECTape files have 129 12-bit words per block of which 128 are used by OS/8.
       This means that we have to write two DECTAPE_BLOCK_SIZE segments containing one extra 12-bit
       garbage word for each OS/8 block of 256 12-bit word.
    */

    pdp8_word_t *word_ptr = block_buffer;
    unsigned byte_offset = block_no * DECTAPE_BLOCK_SIZE * 2;

    do {
        unsigned char* byte_ptr = byte_buffer;
        do {
            if ((*word_ptr & 0170000) != 0) {
                printf("Buffer for block %i appears to be corrupted, write aborted\n", block_no);
                return false;
            }
            *byte_ptr++ = *word_ptr & 0377;
            *byte_ptr++ = *word_ptr++ >> 8;
        } while (byte_ptr < byte_buffer + OS8_BLOCK_SIZE);
        *byte_ptr++ = 0; *byte_ptr++ = 0; /* not necessary but make the block look clean */
        unsigned bytes = pwrite(os8_file, byte_buffer, DECTAPE_BLOCK_SIZE, byte_offset);
        if (bytes != DECTAPE_BLOCK_SIZE) {
            return false;
        }
        byte_offset += DECTAPE_BLOCK_SIZE;
    } while (word_ptr < block_buffer + OS8_BLOCK_SIZE);

    return true;
}

bool write_rka_block(int os8_file, unsigned block_no, os8_block_t block_buffer)
{
    byte_buffer_t byte_buffer;

    pdp8_word_t w1, w2;
    pdp8_word_t *block_ptr = block_buffer;
    unsigned char *buf_ptr = byte_buffer;

    do {
        w1 = *block_ptr++;
        w2 = *block_ptr++;
        if (((w1 & 0170000) != 0) || ((w2 & 0170000) != 0)) {
            printf("Buffer for block %i appears to be corrupted, write aborted\n", block_no);
            return false;
        }
        *buf_ptr++ = w1 >> 4;
        *buf_ptr++ = ((w1 & 017) << 4) | (w2 >> 8);
        *buf_ptr++ = w2 & 0377;
    } while (block_ptr < block_buffer + OS8_BLOCK_SIZE);

    unsigned bytes = pwrite(os8_file, byte_buffer, RK05_BLOCK_SIZE, block_no * RK05_BLOCK_SIZE);
    return bytes == RK05_BLOCK_SIZE;
}

bool write_rkb_block(int os8_file, unsigned block_no, os8_block_t block_buffer)
{
    return write_rka_block(os8_file, block_no + RK05_RKB_OFFSET, block_buffer);
}

bool read_dsk_block(int os8_file, unsigned block_no, os8_block_t block_buffer)
{
    /* It takes two bytes to make a 12-bit word ... */
    byte_buffer_t byte_buffer;
    int bytes = pread(os8_file, byte_buffer, OS8_BLOCK_SIZE * 2, block_no * OS8_BLOCK_SIZE * 2);
    if (bytes != OS8_BLOCK_SIZE * 2) {
        return false;
    }
    return byte_buffer_to_word_buffer(block_no, byte_buffer, block_buffer);
}

bool read_dectape_block(int os8_file, unsigned block_no, os8_block_t block_buffer)
{
    byte_buffer_t byte_buffer;

    /* Unconverted simh DECTape files have 129 words per block of which 128 are used by OS/8 */
    unsigned char *byte_ptr = byte_buffer;
    unsigned byte_offset = block_no * DECTAPE_BLOCK_SIZE * 2;

    do {
        unsigned bytes = pread(os8_file, byte_ptr, OS8_BLOCK_SIZE, byte_offset);
        if (bytes != OS8_BLOCK_SIZE) {
            return false;
        }
        byte_ptr += OS8_BLOCK_SIZE;
        byte_offset += DECTAPE_BLOCK_SIZE;
    } while (byte_ptr < byte_buffer + OS8_BLOCK_SIZE * 2);

    return byte_buffer_to_word_buffer(block_no, byte_buffer, block_buffer);
}

bool read_rka_block(int os8_file, unsigned block_no, os8_block_t block_buffer)
{
    byte_buffer_t byte_buffer;
    int bytes = pread(os8_file, byte_buffer, RK05_BLOCK_SIZE, block_no * RK05_BLOCK_SIZE);
    if (bytes != RK05_BLOCK_SIZE) {
        return false;
    }

    unsigned char c1, c2, c3;
    pdp8_word_t *block_ptr = &block_buffer[0];
    unsigned char *buf_ptr = &byte_buffer[0];
    do {
        c1 = *buf_ptr++;
        c2 = *buf_ptr++;
        c3 = *buf_ptr++;
        *block_ptr++ = (pdp8_word_t)((c1 << 4) | (c2 >> 4));
        *block_ptr++ = (pdp8_word_t)(((c2 & 017) << 8) | c3);
    } while (block_ptr < block_buffer + OS8_BLOCK_SIZE);

    return true;
}

bool read_rkb_block(int os8_file, unsigned block_no, os8_block_t block_buffer)
{
    return read_rka_block(os8_file, block_no + RK05_RKB_OFFSET, block_buffer);
}

/* Read, write, and create directories */

bool read_directory(block_io_t read_block, int os8_file, directory_t directory)
{
    int block_no = FIRST_DIR_BLOCK;

    do {
        if (!read_block(os8_file, block_no, directory[block_no - FIRST_DIR_BLOCK].d.data)) {
            return false;
        };
        int i = block_no - FIRST_DIR_BLOCK;
        directory[i].dirty = false;
        block_no = directory[i].d.dir_struct.next_segment;
        if (block_no > DIR_LENGTH) {
            return false;
        }
    } while (block_no != 0);

    if (!validate_directory(directory)) {
        return false;
    }

    return true;
}

bool write_directory(block_io_t write_block, int os8_file, directory_t directory)
{

    if (!validate_directory(directory)) {
        printf("Internal error, directory will not be written\n");
        return false;
    }

    int block_no = FIRST_DIR_BLOCK;

    do {
        int i = block_no - FIRST_DIR_BLOCK;
        if (directory[i].dirty) {
            if ((directory[i].d.dir_struct.next_segment > DIR_LENGTH) ||
                !write_block(os8_file, block_no, directory[block_no - FIRST_DIR_BLOCK].d.data)) {
                printf("Error writing directory, directory may be corrupted\n");
                return false;
            }
        };
        directory[i].dirty = false;
        block_no = directory[i].d.dir_struct.next_segment;
    } while (block_no);

    return true;
}

bool get_device(device_t *device, format_t format)
{
    switch (format) {
        case dectape:
        case tu56:
            device->last_block_no = DECTAPE_BLOCKS - 1;
            device->filesystem_size = DECTAPE_BLOCKS - FIRST_DIR_BLOCK - DIR_LENGTH;
            break;
        case rk05:
            device->last_block_no = RK05_RKB_OFFSET - 1;
            device->filesystem_size = RK05_RKB_OFFSET - FIRST_DIR_BLOCK - DIR_LENGTH;
            break;
        default:
            printf("Internal error: unsupported device format\n");
            return false;
    }   
    device->size = device->last_block_no + 1;
    return true;
}

/* Zero empties an existing filesystem, preserving the system blocks if it is a system
   disk. Zero is a rubber mallet.

   Create writes a new directory on the device file.  Create is a sledgehammer.

   Main program takes care of flushing the dirty directory blocks.
*/

bool zero_filesystem(directory_t directory, format_t format)
{
    device_t device;
    get_device(&device, format);

    directory[0].d.dir_struct.number_files = negate(1);
    directory[0].d.dir_struct.next_segment = 0;
    directory[0].d.dir_struct.flag_word = 0;
    directory[0].d.dir_struct.file_entries[0] = 0; /* empty file */
    directory[0].d.dir_struct.file_entries[1] =
        negate(device.size - directory[0].d.dir_struct.first_file_block);
    directory[0].dirty = true;

    return true;
}

bool create_filesystem(block_io_t write_block, int os8_file,
                      directory_t directory, format_t format)
{

    device_t device;
    get_device(&device, format);

    for (dir_block_t *block_ptr = &directory[0]; block_ptr < directory + DIR_LENGTH;
         block_ptr++) {
        block_ptr->dirty = false;
        for (pdp8_word_t *data_ptr = &(block_ptr->d.data[0]); data_ptr < block_ptr->d.data + OS8_BLOCK_SIZE;
             data_ptr++) {
            *data_ptr = 0;
        }
    }

    directory[0].d.dir_struct.number_files = negate(1);
    directory[0].d.dir_struct.first_file_block = DIR_LENGTH + 1;
    directory[0].d.dir_struct.next_segment = 0;
    directory[0].d.dir_struct.flag_word = 0;
    directory[0].d.dir_struct.additional_words = negate(1);
    directory[0].d.dir_struct.file_entries[1] = negate(device.filesystem_size);

    if (!validate_directory(directory)) {
        printf("Error validating directory - are you sure the image file is properly formatted?\n");
        return false;
    }

    /* Write zero blocks in front of the directory. Though we know there's only one at zero,
       generalizing it doesn't hurt anything and the compiler should figure it out.
    */

    for (unsigned block_no = 0; block_no < FIRST_DIR_BLOCK; block_no++) {
        if (!write_block(os8_file, block_no, directory[1].d.data)) {
            printf("Error zeroing device in create filesystem\n");
            return false;
        }
    }

    /* write all of the directory blocks whether active or not when initializing */
    for (unsigned block_no = 0; block_no < DIR_LENGTH; block_no++) {
        if (!write_block(os8_file, block_no + FIRST_DIR_BLOCK,
                         directory[block_no].d.data)) {
            printf("Error writing initial directory in create filesystem\n");
            return false;
        }
    }

    /* now extend the file if necessary */
    if (!write_block(os8_file, device.last_block_no, directory[1].d.data)) {
        printf("Error extending device file in create filesystem\n");
        return false;
    }

    return true;
}

void print_directory(directory_t directory, long columns, const_str_t match_filename,
                     bool print_empties_p)
{
    cursor_t cursor;
    entry_t entry;
    long column = 0;
    pdp8_word_t files = 0;
    pdp8_word_t used = 0;
    pdp8_word_t empty = 0;
    pattern_t pattern;

    build_pattern(match_filename, &pattern);

    init_cursor(directory, &cursor);
    while (valid_entry(&cursor)) {
        get_entry(&cursor, &entry);
        if (entry.empty_file) {
            empty += entry.length;
        }
        if (entry.empty_file && print_empties_p) {
            printf("%-11s", "<empty>");
        } else if (!entry.empty_file && entry.length != 0 && 
                   pattern_match_p(entry.name, pattern)) {
            os8_filename_t filename;
            get_filename(entry.name, filename);
            printf("%-11s", filename);
            used += entry.length;
            files++;
        } else {
            continue;
        }
        column++;
        printf("%5d", entry.length);
        if (column % columns != 0) {
            printf("%10s", " ");
        } else {
            putchar('\n');
        }
    }
    if (column % columns != 0) {
        putchar('\n');
    }
    printf("\n  %d Files In %d Blocks - %d Free Blocks\n", files, used, empty);
}

void delete_entry(entry_t *entry)
{
    /* scrunch the directory segment down to the end of the new
       entry, accounting for the fact that we didn't remove the
       file but will be changing it to an empty file.  The
       order here is important.
    */
    fix_segment_down(*entry, empty_entry_length);
    entry->empty_file = true; 
    put_entry(*entry);        
}

/*
  Makes a new OS/8 file.  It will delete the old one and return an
  entry for an empty file that's at least as large as the size requested.

  It allows one to request the largest available block by passing zero as
  the size, just like OS/8, but this program doesn't use this.

  Don't write more blocks than are contained in the empty file entry that
  is returned!

*/

bool allocate_os8_file(char *filename, unsigned size, directory_t directory, entry_t *entry)
{
    entry_t exclude_entry = {0};
    cursor_t cursor;

    init_cursor(directory, &cursor);
    if (lookup(filename, directory, &cursor, &exclude_entry)) {
        delete_entry(&exclude_entry);
    }

    return get_empty_entry(directory, exclude_entry, entry, size);
}

/* You must call this with the entry passed back by get_empty_file */

bool enter_os8_file(char *filename, unsigned size, directory_t directory, entry_t entry)
{
    if (enter(filename, size, directory, entry)) {
        consolidate(directory);
        return true;
    }
    return false;
}


bool stream_host_image_file(FILE *input, int os8_file, block_io_t write_block,
                            directory_t directory, char *outputname, unsigned size)
{
    os8_block_t block;

    /* Compute size for get_empty_entry */
    unsigned output_size = (size + (OS8_BLOCK_SIZE - 1) * 2)  /
                           (OS8_BLOCK_SIZE * 2);
    entry_t entry;
    if (!allocate_os8_file(outputname, output_size, directory, &entry)) {
        return false;
    }

    unsigned block_no = 0;
    int cnt;
    while ((cnt = fread(block, 2, OS8_BLOCK_SIZE, input)) > 0) {

        /* should never happen */
        if (block_no >= entry.length) {
            return false;
        }

        /* zero out the rest of te  block to avoid "data corrupted" message */
        for (pdp8_word_t *p = block + cnt; p < block + OS8_BLOCK_SIZE;) {
           *p++ = 0;
        } 

        if (!write_block(os8_file, entry.file_block + block_no, block)) {
            return false;
        }
        block_no++;
    }

    return enter_os8_file(outputname, block_no, directory, entry);
}

bool stream_os8_image_file(entry_t entry, int os8_file,
                          block_io_t read_block, FILE *output)
{
    os8_block_t block;
    bool eof_p = false;
    unsigned cnt = 0;

    for (pdp8_word_t block_no = entry.file_block;
        block_no < entry.file_block + entry.length;
        block_no++) {
        if (!read_block(os8_file, block_no, block)) {
            return false;
        }
        if ((cnt = fwrite(block, 2, OS8_BLOCK_SIZE, output)) != OS8_BLOCK_SIZE) {
            return false;
        }
    }

    return true;
}

/* helper for stream_host_text_file since we output <cr> chars before
   <lf> chars in most cases.
*/
bool put_os8_char(FILE *t, pdp8_word_t ch)
{
    static int i = 0;
    static pdp8_word_t w[2];
    switch (i % 3) {
    case 0:
        w[0] = ch;
        break;
    case 1:
        w[1] = ch;
        break;
    case 2:
        w[0] |= (ch & 0360) << 4;
        w[1] |= (ch & 017) << 8;
        if (fwrite(w, sizeof(pdp8_word_t), 2, t) != 2) {
            perror("error writing temp file");
            return false;
        }
        break;
    }
    i++;
    return true;
}

/* This function streams to a temp file, then calls stream_host_image_file
   to write the result to the OS/8 device file.  We do this because we'll
   add <cr>s in front of newlines, which  makes the file longer, which means
   that you can't use the size of the input file to ask for an emply slot
   on the OS/8 filesystem.
*/
bool stream_host_text_file(FILE *input, int os8_file, block_io_t write_block,
                            directory_t directory, char *outputname)
{
    struct stat stat_buf;
    FILE *t = tmpfile();

    char c;
    bool ctrl_z_seen;

    while ((c = fgetc(input)) != EOF && !ctrl_z_seen) {
        bool first_lf = true;
        ctrl_z_seen = c == '\032';
        if (c == '\012'  && first_lf) {
            put_os8_char(t, 0215);
        }
        first_lf = (c != '\012') && (c != '\015');
        if (c != '\0') {
            put_os8_char(t, c | 0200); /* always set the mark bit */
        }
    }

    /* OS/8 will be very unhappy without its ^Z at the end */
    if (!ctrl_z_seen) {
        put_os8_char(t, 0232);
    }

    /* flush partial output */
    put_os8_char(t, 0);
    put_os8_char(t, 0);

    fflush(t);
    fseek(t, 0, SEEK_SET);

    if (fstat(fileno(t), &stat_buf) == -1) {
        perror("stat");
        return false;
    }

    if (!stream_host_image_file(t, os8_file,  write_block, directory,
                                outputname, stat_buf.st_size)) {
        return false;
    }

    fclose(t);

    return true;
}

bool stream_os8_text_file(entry_t entry, int os8_file,
                          block_io_t read_block, FILE *output)
{
    os8_block_t block;
    bool eof_p = false;
    pdp8_word_t block_no = entry.file_block;
    unsigned cnt = 0;

    while (!eof_p) {
        char ch;
        if (!read_block(os8_file, block_no, block)) {
            return false;
        }

        /* unpack one block, stripping the mark bit, ignoring nulls and rubouts, etc */
        pdp8_word_t *block_ptr = block;
        while (!eof_p && block_ptr < block + OS8_BLOCK_SIZE) {
            switch (cnt % 3) {
            case 0:
                ch = *block_ptr & 0177;
                break;
            case 1:
                ch = *(block_ptr + 1) & 0177;
                break;
            case 2:
                ch = ((*block_ptr >> 4) & 0160) | *(block_ptr + 1) >> 8;
                block_ptr += 2;
                break;
            }
            if (ch != 0177 &&
                ch != 015 &&
                ch != 0 &&
                ch != 032) {
                putc(ch, output);
            }
            eof_p = ch == 032;
            cnt++;
        }
        block_no++;
        eof_p |= block_no == entry.file_block + entry.length;
    }
    return true;
}

/* command line processor will only call this for an OS/8 text file */
bool print_os8_text_file(const_str_t filename, int os8_file,
                    block_io_t read_block, directory_t directory)
{
    cursor_t cursor;
    entry_t entry;
    init_cursor(directory, &cursor);
    if (lookup(filename, directory, &cursor, &entry)) {
        return stream_os8_text_file(entry, os8_file, read_block, stdout);
    }
    printf("OS/8 file not found\n");
    return false;
}
bool copy_os8_files(char **argv, int first, int last, int os8_file,
                    block_io_t read_block, directory_t directory)

/* We are guaranteed that the last file is a path to an existing host directory
   for a possibly non-existing file, and that the first through last-1 files are
   all OS/8 file names.
*/
{
    int fd;
    struct stat stat_buf;
    bool is_dir_p;

    if ((fd = open(argv[last], O_RDONLY)) != -1) {
        if (fstat(fd, &stat_buf) == -1) {
            perror("stat");
            return false;
        }
        is_dir_p = S_ISDIR(stat_buf.st_mode);
    }

    /* We will only copy multiple files to a directory, just like the "cp" command
       in Unix-like systems.  If there is a single source file which contains wildcard
       characters we assume it will match multiple files.
    */

    if ((last - first > 1 ||
         (last - first == 1 && os8_contains_wildcard_p(argv[first]))) &&
        !is_dir_p) {
        printf("Output file must be an existing host directory\n");
        return false;
    }

    close(fd);

    for (int i = first; i < last; i++) {
        cursor_t cursor;
        entry_t entry;

        init_cursor(directory, &cursor);
        while (lookup(argv[i], directory, &cursor, &entry)) {
            char output_path[PATH_MAX + 10] = {'\0'};
            os8_filename_t filename;
            strncat(output_path, argv[last], PATH_MAX);
            get_filename(entry.name, filename);

            if (is_dir_p) {
                strcat(output_path, "/");
                strcat(output_path, filename);
            } 

            bool text_p = filename_type(filename) == text_type;
            FILE *output;
            if ((output = fopen(output_path, text_p ? "w" : "wb")) == NULL) {
                perror("Error opening output file:");
                return false;
            }

            bool error_p;
            if (text_p) {
                error_p = !stream_os8_text_file(entry, os8_file, read_block, output);
            } else {
                error_p = !stream_os8_image_file(entry, os8_file, read_block, output);
            }

            fclose(output);

            if (error_p) {
                printf("Error copying OS/8 file %s to %s\n", filename, output_path);
                return false;
            }
        }
    } 
    return true;
}

bool copy_host_files(char **argv, int first, int last, int os8_file,
                    block_io_t write_block, directory_t directory)

/* Copy from the host to the OS/8 device  image file.

   We are guaranteed that the last argument is a legal OS/8 device or
   file spec.

   If there is only one file to copy we can copy to a specific s8 file, otherwise the
   target argument must be "os8:".
*/
{
    /* We will only copy multiple files to "os8:", just like the "cp" command
       in Unix-like systems.
    */

    if ((last - first >= 1 && !os8_devicename_p(argv[last]))) {
        printf("Output file must be \"os8\"\n");
        return false;
    }

    bool text_p;
    bool error_p = false;

    for (int i = first; i < last && !error_p; i++) {
        text_p = filename_type(argv[i]) == text_type;
        FILE *input;
        if ((input = fopen(argv[i], text_p ? "r" : "rb")) == NULL) {
            perror("Error opening input file:");
            return false;
        }

        struct stat stat_buf;

        if (fstat(fileno(input), &stat_buf) == -1) {
            perror("stat of host file failed:");
            return false;
        }

        os8_filename_t outputname = {'\0'};
        if (os8_devicename_p(argv[last])) {
            char *path = strdup(argv[i]);
            char *base = basename(path);
            if (!os8_filename_p(base)) {
                printf("\"%s\" is not a legal OS/8 filename\n", path);
                return false;
            }
            strcat(outputname, base);
        } else {
            strcat(outputname, strip_device(argv[last]));
        }
        if (text_p) {
            error_p = !stream_host_text_file(input, os8_file,  write_block, directory,
                                              outputname);
        } else {
            error_p = !stream_host_image_file(input, os8_file,  write_block, directory,
                                              outputname, stat_buf.st_size);
        }

        if (error_p) {
            printf("Error copying host file %s to OS/8 file %s\n", argv[i], outputname);
            return false;
        }
    }
    return true;
}

bool delete_os8_files(char **argv, int first, int last, bool quiet_p, directory_t directory)

/* We are guaranteed that all of the files on the command line are os8 files,
   possibly wildcarded.

   In theory one could track everything in a single pass but it is simpler to
   delete files and fix the directory structure for each file.  So that's what
   we do.
*/
{
    int deleted_files = 0;
    for (int i = first; i <= last; i++) {
        cursor_t cursor;
        entry_t entry;
        init_cursor(directory, &cursor);
        pattern_t pattern;

        build_pattern(strip_device(argv[i]), &pattern);

        while (valid_entry(&cursor)) {
            peek_entry(cursor, &entry);
            if (!entry.empty_file && entry.length != 0 &&
                pattern_match_p(entry.name, pattern)) {
                bool delete_file_p = true;
                if (!quiet_p) {
                    os8_filename_t filename;
                    get_filename(entry.name, filename);
                    printf("Delete file %s?", filename);
                    delete_file_p = yes_no("");
                }
                if (delete_file_p) {
                    delete_entry(&entry);
                    deleted_files++;
                }
            }
            advance_cursor(&cursor, entry);
        }
    }

    consolidate(directory);

    printf("%d files deleted\n", deleted_files);
    return true;
}


/* Command line processing and main program */

/* make sure all of the referenced files in the command line are either all
   OS/8 or all host files.
*/

bool want_os8_files_p(char *argv[], int first, int last, bool want_os8_p)
{
    for (int i = first; i <= last; i++ ) {
        if (os8_file_spec_p(argv[i]) != want_os8_p) {
            return false;
        }
    }
    return true;
}

bool not_only_once_p(bool flag, char *prefix)
{
    if (flag) {
        printf("%s can only appear once\n", prefix);
        return true;
    }
    return false;
}

void usage() {
    printf("An os8_file file is required with one of the following extensions:\n");
    printf("  .tu56,.dt8 (129 word or 128 word blocks, simh and MAC PDP-8/e compatible)\n");
    printf("  .dsk (simh disk image)\n");
    printf("  .rk05 (Mac PDP-8/e simulator RK05 format)\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[])
{
    char *dot_pos;
    struct stat stat_buf;
    char *os8_devicename = NULL;
    int os8_file;
    directory_t directory;
    block_io_t read_block;
    block_io_t write_block;
    format_t format = unknown;
    rk05_filesystem_t rk05_filesystem = base;
    const_str_t match_filename = "*.*";
    bool print_empties_p = false;

    /* Process command line */

    enum {none, dir, delete, create, zero, copy_to_os8, copy_from_os8, print_from_os8} command;
    bool quiet_p = false;
    long columns = 2;
    bool columns_p = false;
    bool command_err_p = false;
    bool exists_p = false; /* allows one to create a filesystem on an existing file */
    char *temp;
    bool force_text_p = false;
    bool force_image_p = false;

    int c;
    while (1) {
        static struct option long_options[] = {

            /* print directory rather than copy files */
            {"dir", no_argument, 0, 'd'},
            {"columns", required_argument, 0, 'c'},
            {"empties", no_argument, 0, 'e'},


            /* "quiet" say to not ask "are you sure?" for each file */
            {"delete", no_argument, 0, 'x'},
            {"quiet", no_argument, 0, 'q'},

            {"create", no_argument, 0, 'C'},
            {"sys", no_argument, 0, 'S'},
            {"exists", no_argument, 0, 'E'},

            /* Specifies the os8 device */
            {"os8",  required_argument, 0, '8'},
            /* PDP-8/e sim format, override auto detection */
            {"rk05", no_argument, 0, 'K'},

            /* RK05 disks have two file systems, RKAn and RKBn.
               These work with both SIMH and PDP-8/e disk images.
            */
            {"rka", no_argument, 0, 'A'},
            {"rkb", no_argument, 0, 'B'},

            /* Forces DECTape */
            {"tu56", no_argument, 0, 'D'},
            {"dt8", no_argument, 0, 'D'},

            /* Force output mode, overriding auto detection by extension */
            /* ignored at the moment */
            {"text", no_argument, 0, 't'},
            {"image", no_argument, 0, 'i'},

            /* Zero out the directory of an existing file, or create a new one */ 
            {"zero", no_argument, 0, 'Z'},
            {0, 0, 0, 0}
        };

        /* getopt_long stores the option index here. */
        int option_index = 0;

        c = getopt_long (argc, argv, "dKABDtiZYxq8:c:", long_options, &option_index);

        /* Detect the end of the options. */
        if (c == -1) {
            break;
        }

        switch (c) {

        case 'C':
            command_err_p = not_only_once_p(command != none, "--dir/--del/--create/--zero");
            command = create;
            break;

        case 'd':
            command_err_p = not_only_once_p(command != none, "--dir/--del/--create/--zero");
            command = dir;
            break;

        case 'x':
            command_err_p = not_only_once_p(command != none, "--dir/--del/--create/--zero");
            command = delete;
            break;

        case 'Z':
            command_err_p = not_only_once_p(command != none, "--dir/--del/--create/--zero");
            command = zero;
            break;

        case 'E':
            command_err_p = not_only_once_p(exists_p, "--exists");;
            exists_p = true;
            break;
      
        case '8':
            command_err_p = not_only_once_p(os8_devicename != NULL, "--os8");;
            os8_devicename = optarg;
            break;
          
        case 'K':
        case 'D':
            command_err_p = not_only_once_p(format != unknown, "Device flag");
            format = c == 'K' ? rk05 : dectape;
            break;

        case 'A':
        case 'B':
            command_err_p = not_only_once_p(rk05_filesystem != base, "RK05 filesystem flag");
            rk05_filesystem = c =='A' ? rka : rkb;
            break;

        case 'c':
            columns = strtol(optarg, &temp, 10);
            columns_p = true;
            if (*temp != '\0' || columns <= 0) {
                printf("Illegal value for --columns\n");
                command_err_p = true;
            }
            break;

        case 'q':
            command_err_p = not_only_once_p(quiet_p, "--quiet");;
            quiet_p = true;
            break;

        case 't':
            command_err_p = not_only_once_p(force_text_p, "--text");;
            force_text_p = true;
            break;

        case 'i':
            command_err_p = not_only_once_p(force_image_p, "--image");;
            force_image_p = true;
            break;

        case 'e':
            command_err_p = not_only_once_p(print_empties_p, "--empties");;
            print_empties_p = true;
            break;

        case '?':
            /* getopt_long already printed an error message. */
            command_err_p = true;
            break;

        default:
            command_err_p = true;
            break;
        }
    }

    /* A bunch of inelegant sanity checks */

    if (exists_p && command != create) {
        printf("--exists can only be used with the --create switch\n");
        command_err_p = true;
    }

    if (os8_devicename == NULL) {
        printf("OS/8 device file name must be specified\n");
        command_err_p = true;
    }
 
    if (columns_p && command != dir) {
        printf("--columns can only be specified with --dir\n");
        command_err_p = true;
    }

    if (print_empties_p && command != dir) {
        printf("--empties can only be specified with --dir\n");
        command_err_p = true;
    }

    int extra_arg_count = argc - optind;

    switch (command) {
        case dir:
            if (extra_arg_count == 1) {
                if (os8_file_spec_p(argv[optind])) {
                    match_filename = strip_device(argv[optind]);
                } else {
                    printf("File argument to --dir must be an os8 file pattern\n");
                    command_err_p = true;
                }
            } else if (extra_arg_count > 0) {
                printf("Too many files for --dir\n");
                command_err_p = true;
            }
            break;

        case create:
            if (extra_arg_count != 0) {
                printf("Too many files for --dir\n");
                command_err_p = true;
            }
            break;
        
        case zero:
            if (extra_arg_count != 0) {
                printf("Too many files for --zero\n");
                command_err_p = true;
            }
            break;

        case delete:
            if (!want_os8_files_p(argv, optind, argc - 1, true)) {
                printf("Can only delete OS/8 files\n");
                command_err_p = true;
            }
            break;

        case none:
            if (argc - optind == 0) {
                printf("No files to copy\n");
                command_err_p = true;
            } else if (argc - optind == 1) {
                if (os8_file_spec_p(argv[optind]) &&
                    !os8_contains_wildcard_p(argv[optind]) &&
                    filename_type(argv[optind]) == text_type) {
                        command = print_from_os8;
                } else {
                    printf("Filename must be an OS/8 text file with no wild cards\n");
                    command_err_p = true;
                }
            } else if (os8_devicename_p(argv[argc - 1]) || os8_file_spec_p(argv[argc - 1])) {
                command = copy_to_os8;
                if (!want_os8_files_p(argv, optind, argc - 2, false)) {
                    printf("Can only copy host files to an OS/8 file or directory\n");
                    command_err_p = true;
                }
            } else {
                command = copy_from_os8;
                if (!want_os8_files_p(argv, optind, argc - 2, true)) {
                    printf("Can only copy OS/8 files to a host file or directory\n");
                    command_err_p = true;
                }
            }
            break;

        default:
            printf("internal error\n");
            exit(EXIT_FAILURE);
            break;
    }

    if (command_err_p) {
        exit(EXIT_FAILURE);
    }

    /* End of command line processing */

    /* if the user didn't specify the os8 file format, try to figure it out */
    if (format == unknown) {
        if ((dot_pos = strrchr(os8_devicename, '.')) != NULL) {
            if (strcmp(dot_pos, ".tu56") == 0 || strcmp(dot_pos, ".dt8") == 0) {
               format = dectape;
            } else if (strcmp(dot_pos, ".dsk") == 0) {
                format = dsk;
            } else if (strcmp(dot_pos, ".rk05") == 0) {
                format = rk05;
            }
        }
    }

    if (format == unknown) {
        usage();
    }

    int oflags = 0;
    switch (command) {
        case copy_to_os8:
        case delete:
        case zero:
            oflags = O_RDWR;
            break;
        case print_from_os8:
        case copy_from_os8:
        case dir:
            oflags = O_RDONLY;
            break;
        case create:
            oflags = exists_p ? O_WRONLY : O_WRONLY |  O_CREAT | O_EXCL;
            break;
        default:
            printf("Internal error\n");
            exit(EXIT_FAILURE);
    }

    if ((os8_file = open(os8_devicename, oflags,
         S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH)) == -1) {
        perror("Error opening --os8 file");
        exit(EXIT_FAILURE);
    }

    if (flock(os8_file, LOCK_EX | LOCK_NB) == -1) {
        close(os8_file);
        perror("OS/8 file is locked");
        exit(EXIT_FAILURE);
    }

    if ((oflags & O_CREAT) == 0 && format == dectape) {

        /* If DECTape, is the block 128 or 129 words?  This could be done by
           inspecting the file for a valid directory under either format but
           the PDP-8/e DECTape handler checks the file length when a DECTape
           is mounted, so we will too.  DECTape files with 128 word blocks
           are no different than standard Simh disk files.
        */

        if (fstat(os8_file, &stat_buf) == -1) {
            perror("stat");
            exit(EXIT_FAILURE);
        }

        if (format == dectape) {
            if (stat_buf.st_size == OS8_DECTAPE_LENGTH) {
                format = dsk;
            } else if (stat_buf.st_size != DECTAPE_LENGTH) {
                printf("OS/8 DECtape files must be %i bytes long, generic PDP-8 DECTape files %i bytes long\n",
                       OS8_DECTAPE_LENGTH, DECTAPE_LENGTH);
                exit(EXIT_FAILURE);
            }
        }
    }

    /* set up reader and writer */
    switch (format) {
    case dsk:
        read_block = &read_dsk_block;
        write_block = &write_dsk_block;
        break;

    case rk05:
        if (rk05_filesystem == rkb) {
            read_block = &read_rkb_block;
            write_block = &write_rkb_block;
        } else {
            read_block = &read_rka_block;
            write_block = &write_rka_block;
        }
        break;

    case dectape:
        read_block = &read_dectape_block;
        write_block = &write_dectape_block;
        break;

    default:
        printf("internal error\n");
        exit(EXIT_FAILURE);

    }

    if (command != create && !read_directory(read_block, os8_file, directory)) {
        printf("Error while reading directory - are you sure the image file is properly formatted?\n");
        exit(EXIT_FAILURE);
    }

    switch (command) {
    case dir:
        print_directory(directory, columns, match_filename, print_empties_p);
        break;
    case delete:
        if (!delete_os8_files(argv, optind, argc - 1, quiet_p,  directory)) {
            exit(EXIT_FAILURE);
        }
        break;
    case zero:
        if (yes_no_sure()) {
            if (!zero_filesystem(directory, format)) {
                printf("Error zeroing directory\n");
                exit(EXIT_FAILURE);
            }
        }
        break;
    case create:
        if (exists_p && yes_no_sure()) {
            if (!create_filesystem(write_block, os8_file, directory, format)) {
                printf("Error creating directory\n");
                exit(EXIT_FAILURE);
            }
        }
        break;
    case copy_to_os8:
        if (!copy_host_files(argv, optind, argc - 1, os8_file, write_block, directory)) {
            exit(EXIT_FAILURE);
        }
        break;
    case copy_from_os8:
        if (!copy_os8_files(argv, optind, argc - 1, os8_file, read_block, directory)) {
            exit(EXIT_FAILURE);
        }
        break;
    case print_from_os8:
        if (!print_os8_text_file(argv[optind], os8_file, read_block, directory)) {
            exit(EXIT_FAILURE);
        }
        break;
    default:
        printf("Not Yet Implemented\n");
        exit(EXIT_FAILURE);
    }

    if (!write_directory(write_block, os8_file, directory)) {
        exit(EXIT_FAILURE);
    }

}
