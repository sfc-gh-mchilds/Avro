import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import pandas
from IPython.display import display

#schema = avro.schema.parse(open("user.avsc", "rb").read())

#writer = DataFileWriter(open("data/9381046094918170/metadata/snap-1690489854955.avro", "wb"), DatumWriter())
#writer.append({"name": "Alyssa", "favorite_number": 256})
#writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
#writer.close()

reader = DataFileReader(open("data/9381046094918170/metadata/snap-1690489854955.avro", "rb"), DatumReader())
for user in reader:
   df = pandas.DataFrame.from_records(user)
reader.close()

display(df)