import pandas
import fastavro
import copy
import json
import boto3
import jinja2
from IPython.display import display

#placeholders for S3 functionality
def dls3():
    return None

def uls3():
    return None

#if I need to infer the schema from the avro files
def infer_schema(filepath, mode):
    f_schema = []
    return f_schema

def avro_df(filepath, mode):
    # Open file stream
    with open(filepath, mode) as fp:
        # Configure Avro reader
        reader = fastavro.reader(fp)
        # Load records in memory
        records = [r for r in reader]
        # Populate pandas.DataFrame with records
        df = pandas.DataFrame.from_records(records)
        # Return created DataFrame
        return df
    

#datfram = avro_df("data/9381046094918170/metadata/snap-1690489854955.avro", "rb")
datfram = avro_df("data/9381046094918170/metadata/1690489854955-kaNCdvuuBWUJ7vFfNawaZw.avro", "rb")
#display(datfram)
print(datfram.describe)
print(datfram.dtypes)
datfram['data_file'] = datfram['data_file'].astype("string")
print(datfram.dtypes)
df2 = datfram.data_file.str.replace('mstead', 'deet')
print(df2)



#get the 
with open("data/9381046094918170/metadata/1690489854955-kaNCdvuuBWUJ7vFfNawaZw.avro", "rb") as f:
    reader = fastavro.reader(f)
    users_read_back = [user for user in reader]
    metadata = copy.deepcopy(reader.metadata)
    writer_schema = copy.deepcopy(reader.writer_schema)
    schema_from_file = json.loads(metadata['avro.schema'])


print(schema_from_file)
print(writer_schema)