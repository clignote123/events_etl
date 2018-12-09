import gc
import json
import logging
import sys
from abc import abstractmethod, ABC
from collections import OrderedDict

import pandas as pd
import psycopg2


class DateTimeConverter:
    @staticmethod
    def convert(df: pd.DataFrame, datetime_field: str, format: str):
        """Convert datetime field to a specific format

        Args:
            df(pd.DataFrame): pandas DataFrame
            datetime_field(str): field in DataFrame to convert
            format(str): format of datetime filed

        Returns:
            pd.DataFrame: DataFrame with converted fields
        """
        df[datetime_field] = pd.to_datetime(
            df[datetime_field],
            format=format,
            cache=True,
            errors='coerce',
        )

        return df


class DateConverter:
    @staticmethod
    def convert(df: pd.DataFrame, datetime_field: str, format: str):
        """Convert datetime field to a specific format

        Args:
            df(pd.DataFrame): pandas DataFrame
            datetime_field(str): field in DataFrame to convert
            format(str): format of datetime filed

        Returns:
            pd.DataFrame: DataFrame with converted fields
        """
        df = DateTimeConverter.convert(df, datetime_field, format)
        df[datetime_field] = df[datetime_field].dt.date

        return df


class AbstractDataCleaner(ABC):
    """Clean data

    Args:
        config(dict): configuration
    """

    def __init__(self, config: dict):
        self.config = config.get('data_import')

    @abstractmethod
    def clean(self, df):
        """Clean data in DataFrame

        Args:
            df(pd.DataFrame): pandas DataFrame

        Returns:
            pd.DataFrame
        """
        pass


class DuplicatesDataCleaner(AbstractDataCleaner):
    """Drop duplicates in DataFrame
    """

    def clean(self, df):
        df.drop_duplicates(inplace=True)


class InvalidFieldsCleaner(AbstractDataCleaner):
    """Check invalid fields
    """

    def clean(self, df):
        for field_name, field_config in self.config.get('fields').items():
            if field_config.get('type') == "datetime":
                df = DateTimeConverter.convert(
                    df, field_name, field_config.get('format')
                )

            if field_config.get('type') == "date":
                df = DateConverter.convert(
                    df, field_name, field_config.get('format')
                )

            if field_config.get('not_null'):
                df.loc[df[field_name].isna(), EtlProcess.IS_VALID_FIELD] = False

                # return df


class AbstractStorage(ABC):
    """Abstract data storage
    """

    @abstractmethod
    def save_csv(self, file_path, dst_table, csv_separator):
        """Save csv file to storage

        Args:
            file_path(str): pandas DataFrame with data
            dst_table(str): destination table
            csv_separator(str): csv separator
        """
        pass


class PsqlStorage(AbstractStorage):
    """Storage to interact with postgres database

    Args:
        host(str): database host
        dbname(str): database name
        user(str): database user
        password(str): database password
        port(str): database port
    """

    def __init__(self, host: str, dbname: str, user: str, password: str,
                 port: str):
        self._conn = psycopg2.connect(host=host, dbname=dbname, user=user,
                                      password=password, port=port)

    def save_csv(self, file_path: str, dst_table: str, csv_separator: str):
        with open(file_path) as f:
            cur = self._conn.cursor()

            cmd = "COPY {} FROM STDIN WITH DELIMITER '{}' CSV header;".format(dst_table, csv_separator)
            cur.copy_expert(cmd, f)

            self._conn.commit()

    @staticmethod
    def create_from_config(config: dict):
        """Create storage object from config

        Args:
            config(dict): configuration for database

        Returns:
            PsqlStorage
        """
        db_config = config.get('psql_db')

        if not db_config:
            raise ValueError('Invalid config for db')

        return PsqlStorage(
            host=db_config.get('host'),
            dbname=db_config.get('dbname'),
            user=db_config.get('user'),
            password=db_config.get('pwd'),
            port=db_config.get('port'),
        )


class EtlProcess:
    """Etl Process help object
    """

    IS_VALID_FIELD = '_is_valid'

    ETL_CONFIG = 'etl_config.json'

    @staticmethod
    def enrich_data(df: pd.DataFrame):
        """Enrich pandas DataFrame

        Args:
            df(pd.DataFrame): pandas DataFrame
        """
        df[EtlProcess.IS_VALID_FIELD] = True

    @staticmethod
    def get_config():
        """Get config for ETL process

        Returns:
            dict
        """
        with open(EtlProcess.ETL_CONFIG) as f:
            config = json.load(f, object_pairs_hook=OrderedDict)

        return config


class AbstractDataLoader(ABC):
    """Abstract data load for loading data

    Args:
        storage(AbstractStorage): abstract storage to save data
        config(dict): config for saving the data
    """

    def __init__(self, storage: AbstractStorage, config: dict):
        self._config = config
        self._storage = storage

    @abstractmethod
    def load(self, df: pd.DataFrame):
        """Load pandas DataFrame to some storage
        Args:
            df(pd.DataFrame): pandas DataFrame with data
        """
        pass

    def _load_to_storage(self, df: pd.DataFrame, dst_table: str, fields: list):
        """Load pandas DataFrame to storage

        Args:
            df(pd.DataFrame): pandas data frame
            dst_table(str): destination table
            fields(list): list of fields that should be used for loading into the storage
        """
        file_path = '{output_path}/{table_name}.csv'.format(output_path=self._config.get('output_path'),
                                                            table_name=dst_table)
        self._save_df_to_csv(df=df, file_path=file_path, fields=fields)
        self._storage.save_csv(file_path, dst_table, self._config.get('csv_separator'))

    def _save_df_to_csv(self, df: pd.DataFrame, file_path: str, fields: list):
        """Save df to csv file

        Args:
            df(pd.DataFrame): pandas DataFrame
            file_path(str): file path for output
            fields(list): a list of fields to export to the file
        """
        with open(file_path, 'w') as f:
            df.to_csv(
                f,
                index=False,
                columns=list(fields),
                chunksize=100000
            )
        del df
        gc.collect()


class ValidDataLoader(AbstractDataLoader):
    """Data loader for loading valid data
    """

    def load(self, df: pd.DataFrame):
        df = df.loc[df[EtlProcess.IS_VALID_FIELD] == True]

        self._load_to_storage(
            df=df,
            dst_table=self._config.get('destination_table'),
            fields=self._config.get('fields').keys()
        )


class InvalidDataLoader(AbstractDataLoader):
    """Data loader for loading invalid data
    """

    def load(self, df: pd.DataFrame):
        df = df.loc[df[EtlProcess.IS_VALID_FIELD] == False]

        self._load_to_storage(
            df=df,
            dst_table=self._config.get('invalid_data_table'),
            fields=self._config.get('fields').keys()
        )


class ExtractJob:
    """ETL job to Extract data

    Args:
        config(dict): configuration for the job
    """

    INPUT_FILE_DIR = './data'

    def __init__(self, config: dict):
        self._config = config.get('data_import')

    def run(self, file_name: str):
        """Run the job

        Args:
            file_name(str): file name inside data folder

        Returns:
            df(pd.DataFrame): data loaded to pandas DataFrame

        """
        file_path = '{}/{}'.format(self.INPUT_FILE_DIR, file_name)

        chunks = pd.read_csv(file_path, sep=',', chunksize=100000, iterator=True, engine='c')
        df = pd.concat(chunks, ignore_index=True)
        del chunks
        gc.collect()

        return df


class TransformJob:
    """ETL job to transform data

    Args:
        config(dict): config for the job
    """

    def __init__(self, config: dict):
        self._config = config
        self._cleaners = [
            DuplicatesDataCleaner(self._config),
            InvalidFieldsCleaner(self._config)
        ]

    def run(self, df):
        """Run the job

        Args:
            df(pd.DataFrame): pandas DataFrame with data

        Returns:
            df(pd.DataFrame): DataFrame with transformed data
        """
        EtlProcess.enrich_data(df)

        for cleaner in self._cleaners:
            cleaner.clean(df)

        return df


class LoadJob:
    """ETL job to load a data

    Args:
        config(dict): config for the job
    """

    def __init__(self, config):
        storage = PsqlStorage.create_from_config(config)
        self._data_loaders = [
            ValidDataLoader(storage, config.get('data_import')),
            InvalidDataLoader(storage, config.get('data_import'))
        ]

    def run(self, df):
        """Run the job

        Args:
            df(pd.DataFrame): pandas DataFrame to load
        """
        for data_loader in self._data_loaders:
            data_loader.load(df)


def init_logger():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

def run():
    file_name = sys.argv[1]

    if not file_name:
        raise ValueError('Please provide a file name to run the script')

    logging.debug('Start processing file {}'.format(file_name))

    etl_config = EtlProcess.get_config()

    extract_job = ExtractJob(etl_config)
    df = extract_job.run(file_name)

    transform_job = TransformJob(etl_config)
    df = transform_job.run(df=df)

    load_job = LoadJob(etl_config)
    load_job.run(df)

    logging.debug('Processing is finished')


if __name__ == '__main__':
    init_logger()
    run()
