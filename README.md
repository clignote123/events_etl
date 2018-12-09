# Etl process for loading events data

## App structure

```bash
-- data/ - dirrectory with input data
-- docker/ - docker configuration
-- .env.dist - distribution file for specifying envrinoment variables
-- docker-compose.yml
-- etl_process.py - main file for runnig etl process with an input file
```

## Requirements

- Python 3.7
- For Docker you mind need to have around 3gb RAM. 

## How to run with docker

1. Init `.env` file by executing a command and specifying values for env variables(you can keep it the same). The same settings for the database should be in the config file `etl_config.json` 
```bash
cp .env.dist .env
```
2. You need to place a file with data to `./data/` folder. File should be a valid csv file
3. You need to create all containers by running
```bash
docker-compose up
```
4. Run `etl_process.py` inside docker container passing like first parameter your file name with events data
```bash
docker-compose run etl python etl_process.py events.csv
```
5. When script is done you can go to `database` container and check loaded data according the tables

## How to run locally

1. Install requirements from `requirements.txt`
2. Specify database configuration in `etl_config.json`
3. Run etl process:
```bash
python etl_process.py events.csv
```
4. When script is done you can go to `database` container and check loaded data according the tables

## Database structure

- Table `event.data` contains valid data from the loaded file
- Table `event.invalid_data` contains invalid data from the loaded file
 
## Pitfalls

- In case you will see `Killed` during running app inside docker  - you might need to increase allowed memory for your docker

## Key concepts

I introduced a few concepts for the application

### Jobs

Main classes(`ExtractJob`, `TransformJob`, `LoadJob`) responsible for creating ETL pipeline

### Data cleaners

Responsible for cleaning data based on requirements, however at some points we need to keep invalid data for analysis therefore `InvalidFieldsCleaner` marks invalid data like `_is_valid=False` which late we could use for extracting valid and invalid data from dataframe


### DataConverters

Converts data from one type to another(`DateTimeConverter`, `DateConverter`)

### Data loaders

Responsible for loading data to some storage. In our case we have tow data loaders: 
- `ValidDataLoader` - loading valid data to a storage
- `InvalidDataLoader` - loading invalid data to a storage


## Configuration

```json
{
  "psql_db": { // database config
  ...
  },
  "data_import": { // config to be used for import
    "csv_separator": ",",
    "destination_table": "events.data",
    "invalid_data_table": "events.invalid_data",
    "output_path": "/tmp",
    "fields": { // configuration for fields that we want to use during the ETL process
    }
  }
}    
```