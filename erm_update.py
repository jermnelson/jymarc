"""
 mod:`erm_update`: Update Electronic Stub Records from a CSV file
                   with URL, ISBN, and other fields extracted from an ILS
                   ERM and checkin records.
"""
__author__ = "Jeremy Nelson"
import csv,re
import xml.etree.ElementTree as ElementTree
CSV_FILE = 'tutt-checkin.csv'

holding_re = re.compile(r"(\d+.\d+)\s*(\d*-*\d*)\s*(\d*-*\d*)\s*(\d*-*\d*)")
volume_re = re.compile(r"(\d+[.]\d+)")
issue_re = re.compile(r"(\d{1,2}-\d{0,2})")
year_re = re.compile(r"(\d{3,4}-\d{0,4})")

def load_csv(csv_file=CSV_FILE):
    """
    Method parses through CSV file and updates electronic journals dict with
    the bib number as the key and the urls, holdings, and issn (if present) for
    look-up by the MARC parser.

    :param csv_file: Common separated file, defaults to settings values
    """
    electronic_bibs = {}
    csv_reader = csv.reader(open(csv_file,'rb'))
    for row in csv_reader:
        row_dict = {}
        urls,paired_holdings,counter = [],[],0
        bib_id = row[0][0:-1] # Removes last digit as per ILS convention
        if len(row[2]) > 1:
            row_dict['issn'] = row[2]
        reversed_fields = row[3:]
        reversed_fields.reverse()
        for value in reversed_fields:
            holdings = []
            if value.lower().startswith('http'):
                raw_url = value.split(' ') # Attempts to split out text from url
                
                urls.append(raw_url[0])
                paired_holdings.append("{0} ".format(' '.join(raw_url[1:])))
            else:
                try:
                    int(value[0]) # Assumes holdings starts with an int
                    pretty_holdings = ''
                    for token in value.split(" "):
                        if volume_re.search(token):
                            pretty_holdings += "v.{0} ".format(volume_re.search(token).groups()[0])
                        elif year_re.search(token):
                            pretty_holdings += "{0} ".format(year_re.search(token).groups()[0])
                        elif issue_re.search(token):
                            pretty_holdings += "n.{0} ".format(issue_re.search(token).groups()[0])
                        else:
                            pretty_holdings += "{0} ".format(token)

                    # Assumes that holdings statement is non-standard, set
                    # raw value
                    paired_holdings[counter] = '''<a href="{0}">{1}</a> {2}'''.format(urls[counter],
                                                                                      paired_holdings[counter],
                                                                                      pretty_holdings)
                    counter += 1
                except:
                    pass
        row_dict['url'] = urls
        row_dict['holdings'] = paired_holdings
        electronic_bibs[bib_id] = row_dict
    return electronic_bibs   
                
                
        
