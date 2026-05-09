# F1 Telemetry Converter

This project is a Spark-based utility designed to parse and convert [this F1 2025 dataset](https://github.com/TracingInsights/2025) 
from raw JSON format into CSV files for easier analysis and processing.

## Configuration

Before running the application, you must configure your local environment paths and AWS S3 settings in the `utils.Config` file.
This ensures the application knows where to read the datasets from and where the project is located.

1. Open `src/main/scala/utils/Config.scala`.
2. Update the variables to match your machine's directories:

```scala
  val projectDir: String = "path/to/the/project"
  val datasetPath: String = "path/to/the/dataset"
```

## Customizing the Conversion
The main entry point for the conversion process is the ```DatasetsConverter``` object. You can easily activate or deactivate the conversion of specific parts of the dataset depending on your needs.

To do this, open `src/main/scala/DatasetsConverter.scala` and comment or uncomment the relevant parser classes inside the main function.
### Telemetry Data
`DatasetParser` class. (Active by default)
### Lap Times
`LapTimesParser` class
### Drivers
`DriversParser` class

## Running the Project
Once your configuration is set and you have chosen which datasets to parse, 
the parsed CSV files will be generated in the `/output` directory defined in the script.