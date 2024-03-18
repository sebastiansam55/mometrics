import zlib
import struct
import bson
import argparse
import sys
from bson.json_util import dumps

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Input metrics file")
    parser.add_argument("-o", "--output", default="metrics.json", help="Output json file")
    args = parser.parse_args()
    print(f"Starting {args}")

    with open(args.input, 'rb') as bson_data_file:
        # bsondata_size = f.read(4)
        # size = struct.unpack('<I', bsondata_size)[0]
        # print(size)
        # entire metrics file is bson data, decode this first
        json_data = bson.decode_all(bson_data_file.read())
        index = 0
        metrics = []
        for line in json_data:
            data_index = 0
            # each line is a json document, where the data key contains a bunch of different information concatenated together
            if index > 0:
                # the first 4 bytes of data are the size of the next bson document stored as little endian byte, unpack and decompress
                zlib_size_int = struct.unpack('<I', line['data'][:4])[0]
                zlib_data = line['data'][4:]
                output = zlib.decompress(zlib_data)

                # retrieve metrics document
                bson_size_int = struct.unpack('<I', output[:4])[0]
                print(f"zlib document size: {zlib_size_int}, output size: {len(output)}, metrics document size: {bson_size_int}")
                metrics_document = bson.decode(output[:bson_size_int])
                metrics.append(metrics_document)

                # read count of metrics
                count_of_metrics = struct.unpack('<I', output[bson_size_int:bson_size_int+4])[0]

                # read count of samples
                count_of_samples = struct.unpack('<I', output[bson_size_int+4:bson_size_int+8])[0]
                
                print(f"count of metrics: {count_of_metrics}, count of samples: {count_of_samples}")

                

            else:
                metrics.append(line)

            index += 1
        with open(args.output, 'w') as out:
                for line in metrics:
                    out.write(dumps(line)+"\n")
