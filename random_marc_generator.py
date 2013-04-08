"""
 Creates a MARC test file of 10,000 records from a full MARC record
 export
"""
import argparse
import datetime
import os
import random
import sys
sys.path.append(os.path.join("lib",
                             "marc4j.jar"))
import java.io.FileInputStream as FileInputStream
import java.io.FileOutputStream as FileOutputStream
import org.marc4j as marc4j
from sharder import check_suppressed

arg_parser = argparse.ArgumentParser(description='Generate Random file of MARC Records')
arg_parser.add_argument('input_marc', help="[input_marc] File path to input MARC file")
arg_parser.add_argument('name', default=None, help="name to be used in testing and training sets")

def old_file_generator():
    print("STARTING MARC Random test file generator")
    random_rec_positions = []
    for i in range(0,REC_RANGE):
        random_rec_positions.append(random.randrange(0,MAX_RECS))
    random_rec_positions.sort()
    marc_file = pymarc.MARCReader(file(INPUT_MARC_FILE,'rb'))
    all_marc_recs = []
    count = 0
    marc_writer = pymarc.MARCWriter(file(OUPUT_MARC_FILE,'w'))
    print("\tStart cycling through MARC file")
    for record in marc_file:
        if random_rec_positions.count(count) > 0:
            try:
                marc_writer.write(record)
            except:
                print("Error writing record %s" % count)
            if random_rec_positions.index(count) % 1000:
                sys.stderr.write(".")
            else:
                sys.stderr.write('%s:%s' % (str(count),
                                            str(random_rec_positions.index(count))))
        count += 1
    print("\tFinished cycling through MARC file, generating random output file")
    marc_writer.close()
    print("FINISHED MARC Random test file generator")

def create_sets(filepath, name=None):
    """
    Function creates a test and a training set from a single MARC file.
    Randomly assigns a record to either the test or training sets.

    :param filepath: File path and name to the MARC21 file
    :param name: Name to be used in test and training set
    """
    if name is None:
        name = os.path.splitext(filepath)[0]
    marc_file = FileInputStream(filepath)
    marc_reader = marc4j.MarcStreamReader(marc_file)
    test_writer = marc4j.MarcStreamWriter(
        FileOutputStream("testing-{0}.mrc".format(name)))
    test_writer.setConverter(marc4j.converter.impl.AnselToUnicode())
    training_writer = marc4j.MarcStreamWriter(
        FileOutputStream("training-{0}.mrc".format(name)))
    training_writer.setConverter(marc4j.converter.impl.AnselToUnicode())
    count = 0
    print('''Starting creation of Training and Testing sets
for {0} at {1}'''.format(name, datetime.datetime.utcnow().isoformat()))
    while marc_reader.hasNext():
        count += 1
        try:
            record = marc_reader.next()
            if check_suppressed(record) is False:
                if random.random() >= .5:
                    test_writer.write(record)
                else:
                    training_writer.write(record)
        except:
            error_msg = 'Failed to write record {0}, error {1}'.format(count,
                                                                       exc_info()[0])
            print(error_msg)
        if not count%1000:
            sys.stderr.write(".{0}.".format(count))
    test_writer.close()
    training_writer.close()
    print('''Finished creating Training and Testing Sets
for {0} at {1}'''.format(name, datetime.datetime.utcnow().isoformat()))
    
    

if __name__ == '__main__':
    args = arg_parser.parse_args()
    input_marc_file = args.input_marc
    name = args.name
    create_sets(input_marc_file, name)
    
    

    
