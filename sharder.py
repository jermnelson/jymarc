"""
 Creates a MARC records shards from a full MARC record load using the jython
 and MARC4J
"""
__author__ = "Jeremy Nelson"

import sys,argparse,datetime,os
sys.path.append(os.path.join("lib",
                             "marc4j.jar")) # Assumes MARC4j jar is in the same directory
import java.io.FileInputStream as FileInputStream
import java.io.FileOutputStream as FileOutputStream
import org.marc4j as marc4j
import codecs

arg_parser = argparse.ArgumentParser(description='Index MARC records into Solr')
arg_parser.add_argument('filename',
                        nargs="+",
                        help="[filename] Name of MARC file to be shared")
arg_parser.add_argument('--shard_size',
                        nargs="+",
                        default=50000,
                        help="[shard_size] Size of shard, default is 50000")

def shard(shard_size,input_marc_filename):
    marc_file = FileInputStream(input_marc_filename)
    marc_reader = marc4j.MarcStreamReader(marc_file)
    count,error_count = 0,0
    marc_output_filename = os.path.join('shards',
                                        'shard-{0}k-{1}.mrc'.format(count,
                                                                    count+shard_size))
    marc_writer = marc4j.MarcStreamWriter(FileOutputStream(marc_output_filename))
    error_log = open('errors.log','w')
    while marc_reader.hasNext():
        try:
            count += 1         
            record = marc_reader.next()
            marc_writer.write(record)
            if not count%shard_size: # Close current output file and open new
                marc_writer.close()
                new_output_filename = os.path.join('shards',
                                                   'shard-{0}k-{1}.mrc'.format(count,
                                                                               shard_size+count))
                output_file = FileOutputStream(new_output_filename)
                print("Starting new shard {0}".format(new_output_filename))
                marc_writer = marc4j.MarcStreamWriter(output_file)
            if count%1000:
                sys.stderr.write(".")
            else:
                sys.stderr.write(str(count))
        except:
            error_count += 1
            error_msg = 'Failed to write record {0}, error {1}'.format(count,
                                                                       sys.exc_info()[0])
            print(error_msg)
            error_count += 1
            error_log.write("{0}\n".format(error_msg))
    error_log.close()
    marc_writer.close()
    print("Finished sharding at {0}, total record={1}, errors={2}".format(datetime.datetime.today().isoformat(),
                                                                          count,
                                                                          error_count))
    
        
                          
    
if __name__ == '__main__':
    args = arg_parser.parse_args()
    if 'shard_size' is args:
        SHARD_SIZE = args.shard_size
    else:
        SHARD_SIZE = 50000 # Default
        print("Using default shard_size of %s" % SHARD_SIZE)
    shard(SHARD_SIZE,args.filename[0])
    
