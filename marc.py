# -*- coding: cp1252 -*-
"""
  `mod`:marc - A jython MARC record parser, loosely based on heavily
  customized MARC parser in the
  `Aristotle Discovery Layer <https://github.com/jermnelson/Discover-Aristotle>`_
  with MARC4J and SolrJ Java libraries to improve unicode processing in
  a MARC 21 record load
"""


import csv,re,sys,time,datetime,os
import urllib,urllib2,codecs
PROJECT_DIR = os.getcwd()
JAR_DIR = os.path.join(PROJECT_DIR,
                       "lib")
for jar_file in os.listdir(JAR_DIR):
    sys.path.append(os.path.join(JAR_DIR,
                                 jar_file))
import pysolr
import xml.etree.ElementTree as et
import java.lang.System as System
import java.io.FileInputStream as FileInputStream
import java.io.FileOutputStream as FileOutputStream
import org.marc4j as marc4j
import org.apache.solr.client.solrj.SolrServerException as SolrServerException
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer as CommonsHttpSolrServer
import org.apache.solr.common.SolrInputDocument as SolrInputDocument
import unicodedata,urllib,logging
from erm_update import load_csv

##logging.basicConfig(filename='%slog/%s-marc-solr-indexer.log' % (settings.BASE_DIR,
##                                                                 datetime.datetime.today().strftime('%Y%m%d-%H')),
##                    level=logging.INFO)

ELECTRONIC_JRNLS = load_csv()


try:
    set
except NameError:
    from sets import Set as set

# local libs
import marc_maps,tutt_maps

ISBN_RE = re.compile(r'(\b\d{10}\b|\b\d{13}\b)')
LOCATION_RE = re.compile(r'\(\d+\)')
NONINT_RE = re.compile(r'\D')
REF_LOC_RE = re.compile(r'(tarf*)')
PER_LOC_RE = re.compile(r'(tper*)')
UPC_RE = re.compile(r'\b\d{12}\b')

FIELDNAMES = [
    'access',
    'audience',
    'author',
    'bib_num',
    'callnum',
    'callnumlayerone',
    'collection',
    'contents',
    'corporate_name',
    'ctrl_num',
    'description',
    'era',
    'format',
    'full_title',
    'full_lc_subject',
    'genre',
    'holdings',
    'id',
    'imprint',
    'isbn',
    'issn',
    'item_ids',
    'language',
    'language_dubbed',
    'language_subtitles',
    'lc_firstletter',
    'location',
    'marc_record',
    'oclc_num',
    'notes',
    'personal_name',
    'place',
    'publisher',
    'publisher_location',
    'pubyear',
    'series',
    'summary',
    'title',
    'title_sort',
    'topic',
    'upc',
    'url',
]


class RowDict(dict):
    """
    Subclass of dict that joins sequences and encodes to utf-8 on get.
    Encoding to utf-8 is necessary for Python's csv library because it
    can't handle unicode.
    >>> row = RowDict()
    >>> row['bob'] = ['Montalb\\xe2an, Ricardo', 'Roddenberry, Gene']
    >>> row.get('bob')
    'Montalb\\xc3\\xa1n, Ricardo|Roddenberry, Gene
    """
    def get(self, key, *args):
        value = dict.get(self, key, *args)
        if not value:
            return ''
        if hasattr(value, '__iter__'):
            try:
                value = u'|'.join([x.decode('utf16','ignore') for x in value if x])
            except:
                value = u'|'.join([unicode(x) for x in value if x])
        return value
        # converting to utf8 with yaz-marcdump instead -- it handles
        # oddities better
##        return pymarc.marc8.marc8_to_unicode(value).encode('utf8')
        # convert to unicode if value is a string
        #if type(value) == type(''):
        #    value = unicode(value, 'utf8')
        # converting to NFC form lessens character encoding issues
##        value = unicodedata.normalize('NFC', value)
##        return value.encode('utf8')

class RecordSuppressedError(Exception):
    """
    Custom Exception raised if record is suppressed.
    """

    def __init__(self,value):
        self.value = value

    def __str__(self):
        return repr(self.value)

access_search = re.compile(r'ewww')

def normalize(value):
    """
    Function normalizes value by replacing periods, commas, semi-colons,
    and colons.

    :param value: Raw value
    :rtype: String with punctuations removed
    """
    if value:
        return value.replace('.', '').strip(',:/; ')

def subfield_list(field, subfield_indicator):
    """
    Method takes MARC field and subfield values and returns
    a list of the subfield values

    :param field: MARC field
    :param subfield_indicator: List or char of subfields
    :rtype: List
    """
    subfields = field.getSubfields(subfield_indicator)
    if subfields is not None:
        return [normalize(subfield.getData()) for subfield in subfields]
    else:
        return []

def format_field(field):
    """
    Helper function takes a field and iterates through following
    pymarc.format_field() logic

    :param field: MARC4j field
    """
    if field.tag < '010' and self.tag.isdigit():
        return field.getData()
    fielddata = ''
    for subfield in field.subfields:
        if subfield.code == '6':
            continue
        if not field.tag.startswith('6'):
            fielddata += ' {0}'.format(subfield.getData())
        else:
            if subfield.code not in ('v','x','y','z'):
                fielddata += ' {0}'.format(subfield.getData())
            else:
                fielddata += ' -- {0}'.format(subfield.getData())
    return fielddata.strip()

def get_access(record):
    '''Generates simple access field specific to CC's location codes

    :param record: MARC record
    :rtype: String message
    '''
    field994s = record.getVariableField('994')
    if access_search.search(field994s.toString()):
        return 'Online'
    else:
        return 'In the Library'

def get_author(record):
    """
    Emulates pymarc record.get_author() method for marc4j

    :param record: MARC record
    """
    field100 = record.getVariableField('100')
    if field100 is not None:
        return format_field(field100)
    field110 = record.getVariableField('110')
    if field110 is not None:
        return format_field(field110)
    field111 = record.getVariableField('111')
    if field111 is not None:
        return format_field(field111)
    return None



def get_format(record):
    '''Generates format, extends existing Kochief function.

    :param record: MARC record
    :rtype: String of Format
    '''
    format = ''
    field007 = record.getVariableField('007')
    if field007 is not None:
        field007 = field007.getData()
    else:
        field007 = ''
    leader = record.getLeader().toString()
    if len(leader) > 7:
        if len(field007) > 5:
            if field007[0] == 'a':
                if field007[1] == 'd':
                    format = 'Atlas'
                else:
                    format = 'Map'
            elif field007[0] == 'c':            # electronic resource
                if field007[1] == 'j':
                    format = 'Floppy Disk'
                elif field007[1] == 'r':        # remote resource
                    if field007[5] == 'a':    # has sound
                        format = 'Electronic'
                    else:
                        format = 'Electronic'
                elif field007[1] == 'o' or field007[1] == 'm':      # optical disc
                    format = 'CDROM'
            elif field007[0] == 'd':
                format = 'Globe'
            elif field007[0] == 'h':
                format = 'Microfilm'
            elif field007[0] == 'k': # nonprojected graphic
                if field007[1] == 'c':
                    format = 'Collage'
                elif field007[1] == 'd':
                    format = 'Drawing'
                elif field007[1] == 'e':
                    format = 'Painting'
                elif field007[1] == 'f' or field007[1] == 'j':
                    format = 'Print'
                elif field007[1] == 'g':
                    format = 'Photonegative'
                elif field007[1] == 'l':
                    format = 'Drawing'
                elif field007[1] == 'o':
                    format = 'Flash Card'
                elif field007[1] == 'n':
                    format = 'Chart'
                else:
                    format = 'Photo'
            elif field007[0] == 'm': # motion picture
                if field007[1] == 'f':
                    format = 'Videocassette'
                elif field007[1] == 'r':
                    format = 'Filmstrip'
                else:
                    format = 'Motion picture'
            elif field007[0] == 'o': # kit
                format = 'kit'
            elif field007[0] == 'q':
                format = 'musical score'
            elif field007[0] == 's':          # sound recording
                if leader[6] == 'i':             # nonmusical sound recording
                    if field007[1] == 's':   # sound cassette
                        format = 'Book On Cassette'
                    elif field007[1] == 'd':    # sound disc
                        if field007[6] == 'g' or field007[6] == 'z':
                            # 4 3/4 inch or Other size
                            format = 'Book On CD'
                elif leader[6] == 'j':        # musical sound recording
                    if field007[1] == 's':    # sound cassette
                        format = 'Cassette'
                    elif field007[1] == 'd':    # sound disc
                        if field007[6] == 'g' or field007[6] == 'z':
                            # 4 3/4 inch or Other size
                            format = 'Music CD'
                        elif field007[6] == 'e':   # 12 inch
                            format = 'LP Record'
            elif field007[0] == 'v':            # videorecording
                if field007[1] == 'f':
                    format = 'VHS Video'
                if field007[1] == 'd':        # videodisc
                    if field007[4] == 'v' or field007[4] == 'g':
                        format = 'DVD Video'
                    elif field007[4] == 's':
                        format = 'Blu-ray Video'
                    elif field007[4] == 'b':
                        format = 'VHS Video'
                    else:
                        logging.error("247 UNKNOWN field007 %s for %s" % (field007[4],record.title()))
                elif field007[1] == 'f':        # videocassette
                    format = 'VHS Video'
                elif field007[1] == 'r':
                    format = 'Video Reel'
    # now do guesses that are NOT based upon physical description
    # (physical description is going to be the most reliable indicator,
    # when it exists...)
    field008 = record.getVariableField("008")
    if field008 is not None:
            field008 = field008.getData()
    else:
            field008 = ''
    if leader[6] == 'a' and len(format) < 1:                # language material
        if leader[7] == 'a':
            format = 'Series' # Ask about?
        if leader[7] == 'c':
            format = 'Collection'
        if leader[7] == 'm':            # monograph
            if len(field008) > 22:
                if field008[23] == 'd':    # form of item = large print
                    format = 'Large Print Book'
                elif field008[23] == 's':    # electronic resource
                    format = 'Electronic'
                else:
                    format = 'Book'
            else:
                format = 'Book'
        elif leader[7] == 's':            # serial
            if len(field008) > 18:
                frequencies = ['b', 'c', 'd', 'e', 'f', 'i', 'j',
                               'q', 's', 't', 'w']
                if field008[21] in frequencies:
                    format = 'Journal'
                elif field008[21] == 'm':
                    format = 'Book'
                else:
                    format = 'Journal'
            else:
                format = 'Journal'
    elif leader[6] == 'b' and len(format) < 1:
        format = 'Manuscript'
    elif leader[6] == 'e' and len(format) < 1:
        format = 'Map'
    elif leader[6] == 'c' and len(format) < 1:
        format = 'Musical Score'
    elif leader[6] == 'g' and len(format) < 1:
        format = 'Video'
    elif leader[6] == 'd' and len(format) < 1:
        format = 'Manuscript noted music'
    elif leader[6] == 'j' and len(format) < 1:
        format = 'Music Sound Recordings'
    elif leader[6] == 'i' and len(format) < 1:
        if leader[7] != '#':
            format = 'Spoken Sound Recodings'
    elif leader[6] == 'k' and len(format) < 1:
        if len(field008) > 22:
            if field008[33] == 'i':
                format = 'Poster'
            elif field008[33] == 'o':
                format = 'Flash Cards'
            elif field008[33] == 'n':
                format = 'Charts'
    elif leader[6] == 'm' and len(format) < 1:
        format = 'Electronic'
    elif leader[6] == 'p' and len(format) < 1:
        if leader[7] == 'c':
            format = 'Collection'
        else:
            format = 'Mixed Materials'
    elif leader[6] == 'o' and len(format) < 1:
        if len(field008) > 22:
            if field008[33] == 'b':
                format = 'Kit'
    elif leader[6] == 'r' and len(format) < 1:
        if field008[33] == 'g':
            format = 'Games'
    elif leader[6] == 't' and len(format) < 1:
        if len(field008) > 22:
            if field008[24] == 'm':
                format = 'Thesis'
            elif field008[24] == 'b':
                format = 'Book'
            else:
                thesis_re = re.compile(r"Thesis")
                #! Quick hack to check for "Thesis" string in 502
                desc502 = record.getVariableField("502")
                if desc502 is not None:
                    desc502 = desc502.toString()
                else:
                    desc502 = ''
                if thesis_re.search(desc502):
                    format = 'Thesis'
                else:
                    format = 'Manuscript'
        else:
            format = 'Manuscript'
    # checks 006 to determine if the format is a manuscript
    field006 = record.getVariableField("006")
    if field006 is not None and len(format) < 1:
        field006 = field006.getData()
        if field006[0] == 't':
            format = 'Manuscript'
        elif field006[0] == 'm' or field006[6] == 'o':
            #! like to use field006[9] to further break-down Electronic format
            format = 'Electronic'
    # Doesn't match any of the rules
    if len(format) < 1:
        logging.error("309 UNKNOWN FORMAT Leader: %s" % (leader))
        format = 'Unknown'

    # Some formats are determined by location

    format = lookup_location(record,format)
    return format

def lookup_location(record,format=None):
    """
    Does a look-up on location to determine format for edge cases like annuals in the
    reference area.

    :param record: MARC Record
    :param format: current format
    """
    location_list = locations = record.getVariableFields('994')
    for location in location_list:
         subfield_a = location.getSubfield('a')
         in_reference = REF_LOC_RE.search(subfield_a.getData())
         if in_reference is not None:
              ref_loc_code = in_reference.groups()[0]
              if ref_loc_code != 'tarfc':
                  return "Book" # Classify everything as a book and not journal
         in_periodicals = PER_LOC_RE.search(subfield_a.getData())
         if in_periodicals is not None:
             return "Journal"
    return format

def get_subject_names(record):
    """
    Iterates through record's 600 fields, returns a list of names

    :param record: MARC record, required
    :rtype: List of subject terms
    """
    output = []
    subject_name_fields = record.getVariableFields('600')
    for field in subject_name_fields:
        name = field.getSubfield('a')
        if name is not None:
            name_str = name.getData()
        else:
            name_str = ''
        titles = field.getSubfields('c')
        for title in titles:
            name_str = '{0} {1}'.format(title.getData(),
                                        name_str)
            output.append(name_str.strip())
        numeration = field.getSubfields('b')
        for number in numeration:
            name_str = '%s %s' % (name_str,
                                  number.getData())
            output.append(name_str.strip())
        dates = field.getSubfields('d')
        for date in dates:
            name_str = '%s %s' % (name_str,
                                  date.getData())
            output.append(name_str.strip())
    return output

def parse_008(record, marc_record):
    """
    Function parses 008 MARC field

    :param record: Dictionary of MARC record values
    :param marc_record: MARC record
    """
    field008 = marc_record.getVariableField('008')
    if field008 is not None:
        field008 = field008.getData()
        if len(field008) < 20:
            print("FIELD 008 len=%s, value=%s bib_#=%s" % (len(field008),
                                                           field008,
                                                           record["id"]))
        # "a" added for noninteger search to work
        dates = (field008[7:11] + 'a', field008[11:15] + 'a')
        # test for which date is more precise based on searching for
        # first occurence of nonintegers, i.e. 196u > 19uu
        occur0 = NONINT_RE.search(dates[0]).start()
        occur1 = NONINT_RE.search(dates[1]).start()
        # if both are specific to the year, pick the earlier of the two
        if occur0 == 4 and occur1 == 4:
            date = min(dates[0], dates[1])
        else:
            if dates[1].startswith('9999'):
                date = dates[0]
            elif occur0 >= occur1:
                date = dates[0]
            else:
                date = dates[1]
        # don't use it if it starts with a noninteger
        if NONINT_RE.match(date):
            record['pubyear'] = ''
        else:
            # substitute all nonints with dashes, chop off "a"
            date = NONINT_RE.sub('-', date[:4])
            record['pubyear'] = date
            # maybe try it as a solr.DateField at some point
            #record['pubyear'] = '%s-01-01T00:00:01Z' % date

        audience_code = field008[22]
        if audience_code != ' ':
            try:
                record['audience'] = marc_maps.AUDIENCE_CODING_MAP[audience_code]
            except KeyError as error:
                #sys.stderr.write("\nIllegal audience code: %s\n" % error)
                record['audience'] = ''

        language_code = field008[35:38]
        try:
            record['language'] = marc_maps.LANGUAGE_CODING_MAP[language_code]
        except KeyError:
            record['language']= ''
    return record

def id_match(id_fields, id_re):
    """
    Function matches an ID based on regular expression

    :param id_fields: List of values from the MARC ID field
    :param id_re: ID reqular expression
    :rtype: List of id values
    """
    id_list = []
    for field in id_fields:
        subfield_a = field.getSubfield('a')
        if subfield_a is not None:
            id_str = normalize(field.getSubfield('a').getData())
        else:
            id_str = None
        if id_str:
            id_match = id_re.match(id_str)
            if id_match:
                id = id_match.group()
                id_list.append(id)
    return id_list

def get_languages(language_codes):
    """
    Function extracts language codes and then does a lookup in the
    MARC maps value.

    :param language_codes: List of codes
    :rtype: List
    """
    split_codes = []
    for code in language_codes:
        code = code.getData().lower()
        if len(code) > 3:
            split_code = [code[k:k+3] for k in range(0, len(code), 3)]
            split_codes.extend(split_code)
        else:
            split_codes.append(code)
    languages = []
    for code in split_codes:
        try:
            language = marc_maps.LANGUAGE_CODING_MAP[code]
        except KeyError:
            language = None
        if language:
            languages.append(language)
    return set(languages)


def get_callnumber(record):
    """Follows CC's practice, you may have different priority order
    for your call number."""
    callnumber = ''
    field086 = record.getVariableField('086')
    field099 = record.getVariableField('099')
    field090 = record.getVariableField('090')
    field050 = record.getVariableField('050')
    # First check to see if there is a sudoc number
    if field086 is not None:
        callnumber = field086.getSubfield('a').getData()
    # Next check to see if there is a local call number
    elif field099:
        callnumber = field099.getSubfield('a').getData()
    elif field090:
        callnumber = field090.getSubfield('a').getData()
       # Finally checks for value in 050
    elif field050:
        callnumber = field050.getSubfield('a').getData()
    return callnumber

def get_holdings(record):
    """Extracts serial holding from 850 and 945 fields
    """
    holdings = []
    all945s = record.getVariableFields('945')
    for field in all945s:
        for subfield in field.getSubfields('c'):
            holdings.append(subfield.getData())
    all850s = record.getVariableFields('850')
    for field in all850s:
        for subfield in field.getSubfields('a'):
            if holdings.count(subfield) < 1:
                holdings.append(subfield.getData())
    return holdings

def get_items(record,ils=None):
    """Extracts item id from bib record for web service call
    to active ILS."""
    items = []
    all945s = record.getVariableFields('945')
    for f945 in all945s:
        for y in f945.getSubfields('y'):
            if ils=='III': # Removes starting period and trailing character
                item_id = y.getData()
                items.append(item_id[1:-1])
    return items

lc_stub_search = re.compile(r"([A-Z]+)")

def get_lcletter(record):
    '''Extracts LC letters from call number.'''
    lc_descriptions = []
    callnum = ''
    field050 = record.getVariableField('050')
    field090 = record.getVariableField('090')
    if field050 is not None:
        subfield_a = field050.getSubfield('a')
        if subfield_a is not None:
            callnum += subfield_a.getData()
        subfield_b = field050.getSubfield('b')
        if subfield_b is not None:
            callnum = "{0}{1}".format(callnum,
                                      subfield_b.getData())
    elif field090 is not None: # Per CC's practice
        callnum = field090.getSubfield('a').getData()
    else:
        return None
    lc_stub_result = lc_stub_search.search(callnum)
    if lc_stub_result:
        code = lc_stub_result.groups()[0]
        try:
            lc_descriptions.append(marc_maps.LC_CALLNUMBER_MAP[code])
        except:
            pass
    return lc_descriptions

def get_location(record):
    """Uses CC's location codes in Millennium to map physical
    location of the item to human friendly description from
    the tutt_maps LOCATION_CODE_MAP dict"""
    output = []
    locations = record.getVariableFields('994')
    code = None
    for row in locations:
        try:
            locations_raw = row.getSubfields('a')
            for code in locations_raw:
                code = LOCATION_RE.sub("",code.getData())
                output.append(tutt_maps.LOCATION_CODE_MAP[code])
                if code in tutt_maps.SPECIAL_COLLECTIONS:
                    output.append("Special Collections")
                if code in tutt_maps.GOVDOCS_COLLECTIONS:
                    output.append("Government Documents")
        except KeyError,e:
            logging.info("{0} Location unknown={1}".format(format_field(record.getVariableField('245')),
                                                           e))
            output.append('Unknown')
    return set(output)

def get_subjects(marc_record,record):
    """
    Helper function extracts all 6xx subject fields and adds to
    various facets in the record dict

    :param marc_record: MARC record
    :param record: Dictionary of indexed values
    :rtype dict: Returns modified record dict
    """
    subject_fields = []  # gets all 65X fields
    for tag in ['600', '610', '611', '630', '648', '650',
                '651', '653', '654', '655', '656', '657',
                '658', '662', '690',
                '691', '696', '697', '698', '699']:
        all_fields = marc_record.getVariableFields(tag)
        subject_fields.extend(all_fields)
    eras = []
    genres = []
    topics = []
    places = []
    full_lc_subjects = []
    for field in subject_fields:
        genres.extend(subfield_list(field, 'v'))
        topics.extend(subfield_list(field, 'x'))
        eras.extend(subfield_list(field,'y'))
        places.extend(subfield_list(field, 'z'))
        subfield_a = field.getSubfield('a')
        if subfield_a is not None:
            subfield_a_str = subfield_a.getData()
        else:
            subfield_a_str = ''
        if field.tag == '650':
            if subfield_a_str != 'Video recordings for the hearing impaired.':
                topics.append(normalize(subfield_a_str))
        elif field.tag == '651':
            if subfield_a_str != 'Video recordings for the hearing impaired.':
                places.append(normalize(subfield_a_str))
        elif field.tag == '655':
            if subfield_a_str != 'Video recordings for the hearing impaired.':
                genres.append(normalize(subfield_a_str))
        lc_header = ''
        for subfield_indicator in ('a', 'v', 'x', 'y', 'z'):
            subfield_value = subfield_list(field,subfield_indicator)
            for subfield in  subfield_value:
                lc_header += '%s -- ' % subfield
        if lc_header[-3:] == '-- ':
            lc_header = lc_header[:-3]
        full_lc_subjects.append(lc_header)
        #    more_topics = subfield_list(subfield_indicator)
        #    topics.extend(more_topics)
    # Process through Subject name fields and add to topics
    topics.extend(get_subject_names(marc_record))
    record['genre'] = set(genres)
    record['topic'] = set(topics)
    record['place'] = set(places)
    record['era'] = set(eras)
    record['full_lc_subject'] = set(full_lc_subjects)
    return record

def get_record(marc_record, ils=None):
    """
    Pulls the fields from a MARCReader record into a dictionary.
    >>> marc_file_handle = open('test/marc.dat')
    >>> reader = pymarc.MARCReader(marc_file_handle)
    >>> for marc_record in reader:
    ...     record = get_record(marc_record)
    ...     print record['author']
    ...     break
    ...
    George, Henry, 1839-1897.
    """
    record = {}
    # TODO: split ILS-specific into separate parsers that subclass this one:
    # horizonmarc, iiimarc, etc.
    try:
        if ils == 'III':
            # [1:-1] because that's how it's referred to in the opac
            bib_id = marc_record.getVariableField('907').getSubfield('a').getData()
            if bib_id is None or len(bib_id) < 10:
              # Try to extract bib number from 035
              for field in  marc_record.getVariableFields('035'):
                  sub_a = field.getSubfield('a')
                  if sub_a is not None:
                      sub_a = sub_a.getData()[1:-1]
                      if sub_a.startswith('b') and len(sub_a) == 8:
                          record['id'] = sub_a
            else:
                record['id'] = bib_id[1:-1]

    except AttributeError:
        # try other fields for id?
        #sys.stderr.write("\nNo value in ID field, leaving ID blank\n")
        #record['id'] = ''
        # if it has no id let's not include it
        logging.error("%s: %s not indexed because of AttributeError" % (marc_record['907']['a'],marc_record.title()))
        return
    # Checks and updates record by checking ELECTRONIC_JRNLS
    # for additional information from check-in records
    if ELECTRONIC_JRNLS.has_key(record['id']):
        record_result = ELECTRONIC_JRNLS[record['id']]
        try:
            record.update(record_result)
        except:
            print("ERROR updating record {0}".format(sys.exc_info()[0]))
##    print("\tafter elect_jrnls")
    all999s = marc_record.getVariableFields('999')
    for field999 in all999s:
        suppressed_codes = field999.getSubfields('f')
        for code in suppressed_codes:
            if code.getData() == 'n':
                error_msg = "NOT INDEXING {0} RECORD".format(record['id'])
                raise RecordSuppressedError(error_msg)
    # should ctrl_num default to 001 or 035?
    field001 = marc_record.getVariableField('001')
    if field001 is not None:
        record['ctrl_num'] = field001.getData()
        # there should be a test here for the 001 to start with 'oc'
        try:
            oclc_number = field001.getData()
            oclc_number = oclc_number.replace("|a","")
        except AttributeError:
            oclc_number = ''
        record['oclc_num'] = oclc_number
##    print("\tafter oclc_num")
    record = parse_008(record, marc_record)
    isbn_fields = marc_record.getVariableFields('020')
##    print("\tafter isbn_fields")
    record['isbn'] = id_match(isbn_fields, ISBN_RE)
##    print("\tafter isbn")
    upc_fields = marc_record.getVariableFields('024')
    record['upc'] = id_match(upc_fields, UPC_RE)
##    print("\tafter upc")
    field041 = marc_record.getVariableField('041')
    if field041 is not None:
        language_dubbed_codes = field041.getSubfields('a')
        languages_dubbed = get_languages(language_dubbed_codes)
        record['language_dubbed'] = []
        for language in languages_dubbed:
            if language != record['language']:
                record['language_dubbed'].append(language)
        language_subtitles_codes = field041.getSubfields('b')
        languages_subtitles = get_languages(language_subtitles_codes)
        if languages_subtitles:
            record['language_subtitles'] = languages_subtitles
    record['access'] = get_access(marc_record)
    record['author'] = get_author(marc_record)
##    print("\tafter author")
    record['callnum'] = get_callnumber(marc_record)
##    print("\tafter callnum")
    record['callnumlayerone'] = record['callnum']
    record['format'] = get_format(marc_record)
    if record.has_key('holdings'):
        record['holdings'].extend(get_holdings(marc_record))
    else:
        record['holdings'] = get_holdings(marc_record)
    record['item_ids'] = get_items(marc_record,ils)
    record['lc_firstletter'] = get_lcletter(marc_record)
    record['location'] = get_location(marc_record)
##    print("\tafter location")
    # are there any subfields we don't want for the full_title?
    field245 = marc_record.getVariableField('245')
    if field245 is not None:
        full_title = format_field(field245)
        try:
            nonfiling = int(field245.indicator2)
        except ValueError:
            nonfiling = 0
        record['full_title'] = full_title
        title_sort = full_title[nonfiling:].strip()
        # good idea, but need to convert to unicode first
        #title_sort = unicodedata.normalize('NFKD', title_sort)
        record['title_sort'] = title_sort
        subfield245_a = field245.getSubfield('a')
        if subfield245_a is not None:
            record['title'] = subfield245_a.getData().strip(' /:;')
##    print("\tafter 245")
    field260 = marc_record.getVariableField('260')
    if field260 is not None:
        record['imprint'] = format_field(field260)
        subfield260_a = field260.getSubfield('a')
        if subfield260_a is not None:
            record['publisher_location'] = normalize(subfield260_a.getData())
        subfield260_b = field260.getSubfield('b')
        if subfield260_b is not None:
            record['publisher'] = normalize(subfield260_b.getData())
##        print("\tafter publisher")
        # grab date from 008
        #if marc_record['260']['c']:
        #    date_find = DATE_RE.search(marc_record['260']['c'])
        #    if date_find:
        #        record['date'] = date_find.group()

    description_fields = marc_record.getVariableFields('300')
##    print("\tafter desc")
    record['description'] = [format_field(field) for field in description_fields]
    record['series'] = []
    for tag in ('440', '490'):
        series_fields = marc_record.getVariableFields(tag)
        for field in series_fields:
            subfield_a = field.getSubfield('a')
            if subfield_a is not None:
                record['series'].append(subfield_a.getData())
            subfield_v = field.getSubfield('v')
            if subfield_v is not None:
                record['series'].append(subfield_v.getData())
##    print("\tafter series")
    record['notes'] = []
    for tag in ('500','501','502','503','504','505','506','507',
                '509','510','512','513','514','515','516','517',
                '518','519','521','545','547','590'):
        note_fields  = marc_record.getVariableFields(tag)
        for field in note_fields:
            record['notes'].append(format_field(field))
##    print("\tafter notes")
    contents_fields = marc_record.getVariableFields('505')
    record['contents'] = []
    for field in contents_fields:
        subfield_a = field.getSubfield('a')
        if subfield_a is not None:
            record['contents'].append(subfield_a.getData())
    summary_fields = marc_record.getVariableFields('520')
    record['summary'] = [format_field(field) for field in summary_fields]

##    subjentity_fields = marc_record.getVariableFields('610')
##    subjectentities = multi_field_list(subjentity_fields, 'ab')
    record = get_subjects(marc_record,record)
##    print("after subjects")
    personal_name_fields = marc_record.getVariableFields('700')
    record['personal_name'] = []
    for field in personal_name_fields:
        for code in ['a', 'b', 'c', 'd']:
            subfields = field.getSubfields(code)
            personal_name = ' '.join([x.getData().strip() for x in subfields])
            record['personal_name'].append(personal_name)

    corporate_name_fields = marc_record.getVariableFields('710')
    record['corporate_name'] = []
    for field in corporate_name_fields:
        for code in ['a', 'b']:
            subfields = field.getSubfields(code)
            corporate_name = ' '.join([x.getData().strip() for x in subfields])
            record['corporate_name'].append(corporate_name)
##    print("After corporate name")
    url_fields = marc_record.getVariableFields('856')
    if not record.has_key("url"):
        record['url'] = []
    for field in url_fields:
        url_subfield = field.getSubfields('u')
        for url in  url_subfield:
            record['url'].append(url.getData())
    record['marc_record'] = marc_record.__str__()  # Should output to MARCMaker format
    return record

def get_row(record):
    """Converts record dict to row for CSV input."""
    row = RowDict(record)
    return row

def get_multi(solr_url):
    """Inspect solr schema.xml for multivalue fields."""
    multivalue_fieldnames = []
    solr_schema_url = "{0}admin/file/?file=schema.xml".format(solr_url)
    solr_schema = urllib2.urlopen(solr_schema_url).read()
    schema = et.fromstring(solr_schema)
    fields_element = schema.find('fields')
    field_elements = fields_element.findall('field')
    for field in field_elements:
        if field.get('multiValued') == 'true':
            multivalue_fieldnames.append(field.get('name'))
    return multivalue_fieldnames

def load_solr(csv_file,solr_url):
    """
    Load CSV file into Solr.  solr_params are a dictionary of parameters
    sent to solr on the index request.
    """
    file_path = os.path.abspath(csv_file)
    solr_params = {}
    for fieldname in get_multi(solr_url):
        tag_split = "f.%s.split" % fieldname
        solr_params[tag_split] = 'true'
        tag_separator = "f.%s.separator" % fieldname
        solr_params[tag_separator] = '|'
    solr_params['stream.file'] = file_path
    solr_params['stream.contentType'] = 'text/plain;charset=utf-8'
    solr_params['commit'] = 'true'
    params = urllib.urlencode(solr_params)
    update_url = solr_url + 'update/csv?{0}'.format(params)
    print("\nLoading records into Solr {0}...".format(update_url))
    try:
        ##response = urllib.urlopen(update_url % params)
        response = urllib2.urlopen(update_url)
    except IOError:
        raise IOError, 'Unable to connect to the Solr instance.'
    print "Solr response:"
    print response.read()



def solr_submission(solr_url, marc_filename, ils='III'):
    """
    Uses Solr java library to create a document batch to send to a Solr server

    :param solr_url: URL to solr server
    :param marc_filename: Full path and name of MARC 21 file
    :param ils: ILS, default to III
    """
    marc_file = FileInputStream(marc_filename)
    marc_reader = marc4j.MarcStreamReader(marc_file)
    error_file = FileOutputStream('solr-index-errors-{0}.mrc'.format(
        datetime.datetime.today().strftime("%Y-%m-%d")))
    error_writer = marc4j.MarcStreamWriter(error_file)
    docs,count,error_count,suppressed = [],0,0,0
    start = datetime.datetime.today()
    solr_server = CommonsHttpSolrServer(solr_url)
    while marc_reader.hasNext():
        try:
            count += 1
            marc_record = marc_reader.next()
            record = get_record(marc_record, ils=ils)
            if record is not None:
                solr_doc = SolrInputDocument()
                for key,value in record.iteritems():
                    solr_doc.addField(key,value)
                docs.append(solr_doc)
            if count % 1000:
                sys.stderr.write(".")
            else:
                sys.stderr.write(str(count))
                solr_server.add(docs)
                solr_response = solr_server.commit()
                docs = []
                System.gc()
                sys.stderr.write(" solr-update:{0} time-lapsed: {1} ".format(count,
                (datetime.datetime.now()-start).seconds / 60.0))
        except RecordSuppressedError, e:
            suppressed += 1
            continue
        except Exception, e:
            import traceback, os.path
            tb = traceback.extract_stack()
            traceback.print_exc(tb)
            exc_type,exc_obj,exc_tb = sys.exc_info()
            error = "Failed to process MARC error={0} {1} count={2}\n".format(exc_obj,
                                                                              exc_tb,
                                                                              count)
            error_count += 1
            sys.stderr.write(error)
            return
            if marc_record is not None:
                error_writer.write(marc_record)
    if len(docs) > 0:
        solr_server.add(docs)
        solr_server.commit()
    finished_indexing = datetime.datetime.today()
    total_minutes = (finished_indexing-start).seconds / 60.0
    index_finished_msg = "\nTotal MARC records of {0}\n".format(count)
    index_finished_msg += '''\tIndexed Started:{0}
    Finished:{1}
    Total Time:{2} mins for {3} records per min
    '''.format(start.isoformat(),
        finished_indexing.isoformat(),
        total_minutes,
        count / total_minutes)
    index_finished_msg += "\tErrors:{0} Suppressed:{1}\n".format(error_count,suppressed)
    sys.stderr.write(index_finished_msg)


def py_solr_submission(solr_url, marc_filename, ils='III'):
    """
    Uses solr python library to create a document batch to send to a Solr server

    :param solr_url: URL to solr server
    :param marc_filename: Full path and name of MARC 21 file
    :param ils: ILS, default to III
    """
    marc_file = FileInputStream(marc_filename)
    marc_reader = marc4j.MarcStreamReader(marc_file)
    error_file = FileOutputStream('solr-index-errors-{0}.mrc'.format(datetime.datetime.today().strftime("%Y-%m-%d")))
    error_writer = marc4j.MarcStreamWriter(error_file)
    solr_server = pysolr.Solr(solr_url)
    docs,count,error_count,suppressed = [],0,0,0
    start = datetime.datetime.today()
    while marc_reader.hasNext():
        try:
            count += 1
            marc_record = marc_reader.next()
            record = get_record(marc_record, ils=ils)
            if record is not None:
                docs.append(record)
            if count%1000:
                sys.stderr.write(".")
            else:
                sys.stderr.write(str(count))
            if not count%1500:
                solr_server.add(docs)
                docs = []
                System.gc()
                sys.stderr.write(" solr-update:{0} ".format(count))
        except RecordSuppressedError, e:
            suppressed += 1
            continue
        except Exception, e:
            import traceback, os.path
            tb = traceback.extract_stack()
            traceback.print_exc(tb)
            exc_type,exc_obj,exc_tb = sys.exc_info()
            error = "Failed to process MARC error={0} {1} count={2}\n".format(exc_obj,
                                                                              exc_tb,
                                                                              count)
            error_count += 1
            sys.stderr.write(error)
            return
            if marc_record is not None:
                error_writer.write(marc_record)
    if len(docs) > 0:
        solr_server.add(docs)
    finished_indexing = datetime.datetime.today()
    index_finished_msg = "\nTotal MARC records of {0}\n".format(count)
    index_finished_msg += "\tIndexed Started:{0}\n\tFinished:{1}\n\t Total Time:{2} mins\n".format(start.isoformat(),
                                                                                                   finished_indexing.isoformat(),
                                                                                                   (finished_indexing-start).seconds / 60.0)
    index_finished_msg += "\tErrors:{0} Suppressed:{1}\n".format(error_count,suppressed)
    sys.stderr.write(index_finished_msg)


def csv_solr_submission(solr_url,marc_filename,ils='III'):
    """
    Uses Solrj to create a document batch to send to a Solr server

    :param solr_url: URL to solr server
    :param marc_filename: Full path and name of MARC 21 file
    :param ils: ILS, default to III
    """
    marc_file = FileInputStream(marc_filename)
    marc_reader = marc4j.MarcStreamReader(marc_file)
    error_file = FileOutputStream('solr-index-errors-{0}.mrc'.format(datetime.datetime.today().strftime("%Y-%m-%d")))
    error_writer = marc4j.MarcStreamWriter(error_file)
##    solr_server = CommonsHttpSolrServer(solr_url)
    docs,count,error_count = [],0,0
    start = datetime.datetime.now()
    fieldname_dict = {}
    for fieldname in FIELDNAMES:
        fieldname_dict[fieldname] = fieldname
    import os.path
    csv_filename = 'tmp{0}.csv'.format(os.path.splitext(marc_filename)[0])
    csv_file_handle = open(csv_filename,'wb')
    csv_writer = csv.DictWriter(csv_file_handle,
                                FIELDNAMES)


    csv_writer.writerow(fieldname_dict)
    while marc_reader.hasNext():
        try:
            count += 1
            marc_record = marc_reader.next()
            record = get_record(marc_record, ils=ils)
            if record is not None:
                row = get_row(record)
                if row is not None:
                    csv_writer.writerow(row)
            if count%1000:
                sys.stderr.write(".")
            else:
                sys.stderr.write(str(count))
        except Exception, e:
            import traceback, os.path
            tb = traceback.extract_stack()
            traceback.print_exc(tb)
            exc_type,exc_obj,exc_tb = sys.exc_info()
            error = "Failed to process MARC error={0} {1} count={2}\n".format(exc_obj,
                                                                              exc_tb,
                                                                              count)
            error_count += 1
            sys.stderr.write(error)
            return
            if marc_record is not None:
                error_writer.write(marc_record)
    try:
        csv_file_handle.close()
        finished_indexing = datetime.datetime.now()
        index_finished_msg = "\nTotal MARC records of {0}\n".format(count)
        index_finished_msg += "\tIndexed Started:{0} Finished:{1} Total Time:{2} mins\n".format(start.isoformat(),
                                                                                                finished_indexing.isoformat(),
                                                                                                (finished_indexing-start).seconds / 60.0)
        load_solr(csv_filename,solr_url)
        index_finished_msg += "\tErrors:{0}\n".format(error_count)
        sys.stderr.write(index_finished_msg)
##        start_solr_ingest = datetime.datetime.now()
##        sys.stderror.write("Starting ingesting into Solr {0}\n".format(start_solr_ingest.isoformat()))

        final_time = datetime.datetime.now()
        sys.stderr.write("Finished at {0} for total time of {1}".format(final_time.isoformat(),
                                                                          (final_time-start).seconds / 60))
    except SolrServerException:
        error = "\nError Ingesting docs into Solr: {0}\n".format(sys.exc_info()[0])
        sys.stderr.write(error)
    finally:
        csv_file_handle.close()


##def write_csv(marc_file_handle, csv_file_handle, collections=None,
##        ils='III'):
##    """
##    Convert a MARC dump file to a CSV file.
##    """
##    # This doctest commented out until field names are stable.
##    #>>> write_csv('test/marc.dat', 'test/records.csv')
##    #>>> csv_records = open('test/records.csv').read()
##    #>>> csv_measure = open('test/measure.csv').read()
##    #>>> csv_records == csv_measure
##    #True
##    #>>> os.remove('test/records.csv')
##
##    # TODO: move xml parsing to marcxml parser
##    #if in_xml:
##    #    reader = pymarc.marcxml.parse_xml_to_array(marc_file_handle)
##    #else:
##    reader = pymarc.MARCReader(marc_file_handle)
##    fieldname_dict = {}
##    for fieldname in FIELDNAMES:
##        fieldname_dict[fieldname] = fieldname
##    #for record in reader
##    count = 0
##    logging.info("Started MARC record import into Aristotle")
##    try:
##        writer = csv.DictWriter(csv_file_handle, FIELDNAMES)
##        writer.writerow(fieldname_dict)
##        for marc_record in reader:
##            count += 1
##            try:
##                record = get_record(marc_record, ils=ils)
##                if record:  # skip when get_record returns None
##                    if collections:
##                        new_collections = []
##                        old_record = get_old_record(record['id'])
##                        if old_record:
##                            old_collections = old_record.get('collection')
##                            if old_collections:
##                                new_collections.extend(old_collections)
##                        new_collections.extend(collections)
##                        try:
##                            record['collection'].extend(new_collections)
##                        except (AttributeError, KeyError):
##                            record['collection'] = new_collections
##                    try:
##                        row = get_row(record)
##                        if row is not None:
##                            writer.writerow(row)
##                    except:
##                        exc_type = sys.exc_info()[0]
##                        exc_value = sys.exc_info()[1]
##                        exc_tb = sys.exc_info()[2]
##                        print("CSV Write Error {0} {1} at line {2} count is {3} ".format(exc_type,
##                                                                                         exc_value,
##                                                                                         exc_tb.tb_lineno,
##                                                                                         count))
##                        for k,v in row.iteritems():
##                            print("\t{0} {1}".format(k,v))
##
##            except:
##                exc_type = sys.exc_info()[0]
##                exc_tb = sys.exc_info()[2]
##                if marc_record.title() is not None:
##                    title = marc_record.title()
##                else:
##                    title = marc_record['245'].format_field()
##                error_msg = "\n{0} error at count={1} line num={2}, title is '{3}'".format(exc_type,
##                                                                                           count,
##                                                                                           exc_tb.tb_lineno,
##                                                                                           title.encode('utf8','ignore'))
##                logging.info(error_msg)
##                try:
##                    sys.stderr.write(error_msg)
##                except:
##                    new_exc_type,new_tb = sys.exc_info()[0],sys.exc_info()[2]
##                    sys.stderr.write("\nERROR writing stderror {0}".format(new_exc_type))
##               #raise
##            else:
##                if count % 1000:
##                    sys.stderr.write(".")
##                else:
##                    logging.info("\t%s records processed" % count)
##                    sys.stderr.write(str(count))
##    finally:
##        marc_file_handle.close()
##        csv_file_handle.close()
##    logging.info("Processed %s records.\n" % count)
##    sys.stderr.write("\nProcessed %s records.\n" % count)
##    return count
##
##def get_old_record(id):
##    id_query = 'id:%s' % id
##    params = [
##        ('fq', id_query.encode('utf8')),
##        ('q.alt', '*:*'),
##        ('qt', 'dismax'),
##        ('wt', 'json'),
##    ]
##    urlparams = urllib.urlencode(params)
##    url = '%sselect?%s' % (settings.SOLR_URL, urlparams)
##    try:
##        solr_response = urllib.urlopen(url)
##    except IOError:
##        raise IOError('Unable to connect to the Solr instance.')
##    try:
##        response = simplejson.load(solr_response)
##    except ValueError as e:
##        print(urllib.urlopen(url).read())
##        raise ValueError('Solr response was not a valid JSON object.')
##    try:
##        doc = response['response']['docs'][0]
##    except IndexError:
##        doc = None
##    return doc

