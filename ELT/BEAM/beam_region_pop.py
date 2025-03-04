import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions()

input_file = "countries_of_the_world_clean.csv"
output_file = "output_5.csv"

with beam.Pipeline(options=pipeline_options) as p:
   (
       p
       | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
       | "Parse CSV" >> beam.Map(lambda x: x.split(','))
       # nasza logika
       | "Select columns" >> beam.Map(lambda x: (x[1], int(x[2])))
       | "Sum CSV" >> beam.CombinePerKey(sum)
       # koniec logiki
       | "Write CSV" >> beam.io.WriteToText(output_file, file_name_suffix=".csv")
   )