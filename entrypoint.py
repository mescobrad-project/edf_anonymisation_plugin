from mescobrad_edge.plugins.edf_anonymisation_plugin.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata
import pyedflib

class GenericPlugin(EmptyPlugin):
    def execute_sql_on_trino(self, sql, conn):
        """Generic function to execute a SQL statement"""

        # Get a cursor from the connection object
        cur = conn.cursor()

        # Execute sql statement
        cur.execute(sql)

        # Get the results from the cluster
        rows = cur.fetchall()

        # Return the results
        return rows

    def transform_input_data(self, data, source_name):
        """Transform input data into table suitable for creating query"""

        data = data.reset_index(drop=True)

        # Add rowid column representing id of the row in the file
        data["rowid"] = data.index + 1

        # Insert source column representing name of the source file
        data.insert(0, "source", source_name)

        # Transform table into table with 4 columns:
        # source,rowid, variable_name, variable_value
        data = data.melt(id_vars=["source","rowid"])
        data = data.sort_values(by=['rowid'])

        # As a variable values type string is expected
        data = data.astype({"value":"str"})

        return data

    def upload_data_on_trino(self, schema_name, table_name, data, conn):
        """Create sql statement for inserting data and update
        the table with data"""

        # Iterate through pandas dataframe to extract each row values
        data_list = []
        for row in data.itertuples(index=False):
            data_list.append(str(tuple(row)))
        data_to_insert = ", ".join(data_list)

        # Insert data into the table
        sql_statement = "INSERT INTO iceberg.{schema_name}.{table_name} VALUES {data}"\
            .format(schema_name=schema_name, table_name=table_name, data=data_to_insert)
        self.execute_sql_on_trino(sql=sql_statement, conn=conn)

    def download_file(self, file_path: str) -> None:
        import boto3
        from botocore.client import Config
        import os
        import time

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)

        # Existing non annonymized data in local MinIO bucket
        obj_personal_data = bucket_local.objects.filter(Prefix="edf_data_tmp/", Delimiter="/")

        # Files for anonymization
        files_to_anonymize = [obj.key for obj in obj_personal_data]

        # Download data which need to be anonymized
        for file_name in files_to_anonymize:
            ts = round(time.time()*1000)
            basename, extension = os.path.splitext(os.path.basename(file_name))
            path_download_file = f"{file_path}{basename}_{ts}{extension}"

            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).download_file(file_name, path_download_file)

            # In order to rename the original file in bucket we need to delete it and upload it again
            s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__, "edf_data_tmp/"+os.path.basename(file_name)).delete()
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_file(path_download_file,
                                                                           "EEGs/edf/" + os.path.basename(path_download_file))

    def anonymize_edf_file(self, signals, signal_headers, header, edf_file, new_file_path,
                           to_remove, new_values):
        """Anonymize edf file by removing values of personal data in header fields."""

        import os

        if not len(to_remove) == len(new_values):
            raise AssertionError('Each to_remove must have one new_value')

        # Remove personal information from header
        for new_val, attr in zip(new_values, to_remove):
            if attr in header.keys():
                header[attr] = new_val

        # Write new file with the same content and anonymized header
        new_file_name = new_file_path + "/" + os.path.basename(edf_file)
        pyedflib.highlevel.write_edf(new_file_name, signals, signal_headers, header)

    def extract_metadata(self, signal_headers, list_of_fields):
        """For each signal (channel) within edf file extract the metadata needed for
        filtering signal"""

        import pandas as pd

        result = []
        for i in range(len(signal_headers)):
            signal_data = {field: signal_headers[i].get(field, None) for field in list_of_fields}
            if signal_headers[i].get('prefilter') is not None:
                records = signal_headers[i].get('prefilter').split()
                for record in records:
                    extracted_data = record.split(":")
                    if len(extracted_data) == 2:
                        key, value = extracted_data
                        signal_data[key] = value
                    else:
                        print(f"Values from: {extracted_data} can't be extracted." )
            result.append(signal_data)

        signals_metadata = pd.DataFrame(result)

        return signals_metadata

    def upload_file(self, path_to_anonymized_files: str) -> None:
        import boto3
        from botocore.client import Config
        import os

        s3 = boto3.resource('s3',
                            endpoint_url=self.__OBJ_STORAGE_URL__,
                            aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID__,
                            aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET__,
                            config=Config(signature_version='s3v4'),
                            region_name=self.__OBJ_STORAGE_REGION__)

        for file in os.listdir(path_to_anonymized_files):
            file_to_upload = os.path.join(path_to_anonymized_files, file)
            if os.path.isfile(file_to_upload):
                s3.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_file(file_to_upload, "EEGs/edf/" + file)

    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        """
        Run the anonymisation process of the edf files.
        Extract metadata from the edf files.
        Upload anonymized files to the corresponding storage.
        """
        import os
        import shutil

        from trino.dbapi import connect
        from trino.auth import BasicAuthentication

        # Initialize the connection with Trino
        conn = connect(
            host=self.__TRINO_HOST__,
            port=self.__TRINO_PORT__,
            http_scheme="https",
            auth=BasicAuthentication(self.__TRINO_USER__, self.__TRINO_PASSWORD__)
        )

        # Get the schema name, schema in Trino is an equivalent to a bucket in MinIO
        # Trino doesn't allow to have "-" in schema name so it needs to be replaced
        # with "_"
        schema_name = self.__OBJ_STORAGE_BUCKET__.replace("-", "_")

        # Get the table name
        table_name = self.__OBJ_STORAGE_TABLE__.replace("-", "_")

        path_to_data = "mescobrad_edge/plugins/edf_anonymisation_plugin/anonymize_files/"

        # create temporary folder for storing downloaded files
        os.makedirs(path_to_data, exist_ok=True)

        # Download data to process
        self.download_file(path_to_data)

        # Anonymize files
        path_to_anonymized_file = path_to_data + "anonymized"
        os.makedirs(path_to_anonymized_file, exist_ok=True)

        try:
            for file in os.listdir(path_to_data):
                path_to_file = os.path.join(path_to_data, file)
                if os.path.isfile(path_to_file):

                    # Read the file and get all the data from the original file
                    signals, signal_headers, header = pyedflib.highlevel.read_edf(path_to_file)

                    # Remove personal information from headers
                    remove_values = ["patientname", "birthdate", "patient_additional", \
                                     "patientcode", "admincode", "gender", "sex", \
                                     "technician"]

                    # Set empty values
                    new_values = ["", "", "", "", "", "", "", ""]

                    print("Anonymization started ... ")
                    self.anonymize_edf_file(signals, signal_headers, header, path_to_file,
                                            path_to_anonymized_file, remove_values,
                                            new_values)

                    # Extract metadata information from the edf file
                    list_of_fields_to_extract = ['label','sample_rate','sample_frequency',\
                                                 'prefilter', 'dimension']
                    print("Extracting metadata information ... ")
                    data = self.extract_metadata(signal_headers, list_of_fields_to_extract)

                    # Source name of the original edf file
                    source_name = os.path.basename(path_to_file)

                    # Transform data in suitable form for updating trino table
                    data_transformed = self.transform_input_data(data, source_name)
                    self.upload_data_on_trino(schema_name, table_name, data_transformed, conn)

            # Upload processed data
            self.upload_file(path_to_anonymized_file)
            print("Processing of the edf file is finished.")

        except Exception as e:
            print("EDF processing failed with error: " + str(e))

        finally:
            # Remove folder with downloaded and anonymized files
            shutil.rmtree(os.path.split(path_to_anonymized_file)[0])

        return PluginActionResponse()
