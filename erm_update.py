"""
 mod:`erm_update`: Update Electronic Stub Records from a CSV file
                   with URL, ISBN, and other fields extracted from an ILS
                   ERM and checkin records.
"""
__author__ = "Jeremy Nelson"
import csv,re,datetime
import xml.etree.ElementTree as ElementTree
CSV_FILE = 'tutt-checkin.csv'

holding_re = re.compile(r"(\d+.\d+)\s*(\d*-*\d*)\s*(\d*-*\d*)\s*(\d*-*\d*)")
month_day_re = re.compile(r"(\d+[.]\d+)")
issue_re = re.compile(r"(\d{1,2}-\d{0,2})")




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
                    int(value[0]) # Assumes holdings starts with an 1.1 indicator
                    pretty_holdings = format_holding_stmt(value)
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
                
vol_num_re = re.compile(r"^1.1 (\d{1,2}-\d{0,2})\s*(\d{1,2}-\d{0,2})")
month_day_re = re.compile(r"(\d{1,2})-(\d{0,2})\s*(\d{1,2})-(\d{0,2})$")
year_re = re.compile(r"(\d{3,4})-(\d{0,4})")            
def format_holding_stmt(raw_value):
    """
    Method takes the raw_value from the holdings csv field and creates a
    pretty display for the Discovery Layer

    :param raw_value: Raw value from the csv field
    :rtype: string of formated "pretty" display of the holdings
    """
    def get_date(month,day):
        if len(month) > 0 and len(day) > 0:
            holding_date = datetime.datetime.strptime("{0}-{1}".format(month,day),
                                                      "%m-%d")
            return holding_date.strftime("(%b. %d") # Returns the start of the date display
                                                      # in the format of "(Mon. day" ex:
                                                      # (Jan. 01-
        return ''
    pretty_holdings = ''
    # Performs regex on raw_value to see volume and issue number is present
    if vol_num_re.search(raw_value) is not None:
        volume,number = vol_num_re.search(raw_value).groups()
        if len(volume) > 1:
            volume = volume.replace("-","")
            pretty_holdings += "v.{0}".format(volume)
        if len(number) > 1:
            number = number.replace("-","")
            if len(volume) > 1:
                pretty_holdings += ":"
            pretty_holdings += "no.{0} ".format(number)
    # Extracts and builds the start and end date strings for the holdings statement
    start_str,end_str = '',''
    if month_day_re.search(raw_value) is not None:
        month_start,month_end,day_start,day_end = month_day_re.search(raw_value).groups()
        start_str += get_date(month_start,day_start)
        end_str += get_date(month_end,day_end)
    # Extracts the year(s) and adds start_str and end_str if there are values
    if year_re.search(raw_value) is not None:
        start_year,end_year = year_re.search(raw_value).groups()
        if len(start_year) > 0 and len(start_str) > 0:
            start_str += ", {0})-".format(start_year)
            pretty_holdings += start_str
        if len(end_year) > 0 and len(end_str) > 0:
            end_str += ", {0}) ".format(end_year)
            pretty_holdings += end_str
    return pretty_holdings.strip()
            
        
        
    
        
    
