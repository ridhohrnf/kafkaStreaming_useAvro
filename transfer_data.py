import apache_beam as beam

# Create a pipeline.
pipeline = beam.Pipeline()

# Read the data from Cloud Storage.
(data,) = (
    pipeline
    | 'ReadFromGCS' >> beam.io.ReadFromCloudStorage(
        bucket='my-bucket',
        prefix='my-path',
    )
)

# Write the data to BigQuery.
data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
    table='my-dataset.my-table',
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
)

# Run the pipeline.
pipeline.run()