import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions()

input_file = "countries_of_the_world_clean.csv"
output_file = "output_4.csv"

with beam.Pipeline(options=pipeline_options) as p:
   (
       p
       | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
       | "Parse CSV" >> beam.Map(lambda x: x.split(','))
       # nasza logika
       | "Select columns" >> beam.Map(lambda x: [x[2]])
       | "type"  >> beam.Map(lambda x: int(x[0]))
       | "Filter CSV" >> beam.CombineGlobally(sum)
       # koniec logiki
 #      | "Join ROWS" >> beam.Map(lambda x: ','.join(x))
       | "Write CSV" >> beam.io.WriteToText(output_file, file_name_suffix=".csv")
   )