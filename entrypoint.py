from mescobrad_edge.plugins.edf_anonymisation_plugin.models.plugin import \
    EmptyPlugin, PluginActionResponse, PluginExchangeMetadata
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

    def transform_input_data(self, data, source_name, workspace_id, pseudoMRN,
                             metadata_file_name):
        """Transform input data into table suitable for creating query"""

        data = data.reset_index(drop=True)

        data["pseudoMRN"] = pseudoMRN

        if metadata_file_name is not None:
            data['metadata_file_name'] = metadata_file_name

        # Add rowid column representing id of the row in the file
        data["rowid"] = data.index + 1

        # Insert source column representing name of the source file
        data.insert(0, "source", source_name)

        # Transform table into table with 5 columns:
        # source, rowid, variable_name, variable_value, workspace_id
        data = data.melt(id_vars=["source","rowid"])
        data = data.sort_values(by=['rowid'])

        # As a variable values type string is expected
        data = data.astype({"value":"str"})

        # Add workspace id into workspace column of the table
        data.insert(4, "workspace_id", workspace_id)

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
        sql_statement = "INSERT INTO iceberg.{schema_name}.{table_name} VALUES \
              {data}".format(schema_name=schema_name,
                             table_name=table_name,
                             data=data_to_insert)
        self.execute_sql_on_trino(sql=sql_statement, conn=conn)

    def download_file(self, file_path: str) -> None:
        import boto3
        from botocore.client import Config
        import os
        import time

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=\
                                    self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=\
                                    self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)

        # Existing non annonymized data in local MinIO bucket
        obj_personal_data = bucket_local.objects.filter(Prefix="edf_data_tmp/",
                                                        Delimiter="/")

        # Files for anonymization
        files_to_anonymize = [obj.key for obj in obj_personal_data]

        # Download data which need to be anonymized
        for file_name in files_to_anonymize:
            ts = round(time.time()*1000)
            basename, extension = os.path.splitext(os.path.basename(file_name))
            path_download_file = f"{file_path}{basename}_{ts}{extension}"

            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).download_file(
                file_name, path_download_file)

            # In order to rename the original file in bucket we need to delete
            # it and upload it again
            s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__,
                            "edf_data_tmp/" + \
                                os.path.basename(file_name)).delete()
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_file(
                path_download_file,
                "EEGs/edf/" + os.path.basename(path_download_file))

    def anonymize_edf_file(self, signals, signal_headers, header, edf_file,
                           new_file_path, to_remove, new_values):
        """Anonymize edf file by removing values of personal data in header
        fields."""

        import os

        if not len(to_remove) == len(new_values):
            raise AssertionError('Each to_remove must have one new_value')

        # Remove personal information from header
        for new_val, attr in zip(new_values, to_remove):
            if attr in header.keys():
                header[attr] = new_val

        # Write new file with the same content and anonymized header
        new_file_name = new_file_path + "/" + os.path.basename(edf_file)
        pyedflib.highlevel.write_edf(new_file_name, signals, signal_headers,
                                     header)

    def extract_metadata(self, signal_headers, list_of_fields):
        """For each signal (channel) within edf file extract the metadata needed
        for filtering signal"""

        import pandas as pd

        result = []
        for i in range(len(signal_headers)):
            signal_data = {field: signal_headers[i].get(field, None) \
                           for field in list_of_fields}
            if signal_headers[i].get('prefilter') is not None:
                records = signal_headers[i].get('prefilter').split()
                for record in records:
                    extracted_data = record.split(":")
                    if len(extracted_data) == 2:
                        key, value = extracted_data
                        signal_data[key] = value
                    else:
                        print(f"Values from: {extracted_data} can't be \
                              extracted." )
            result.append(signal_data)

        signals_metadata = pd.DataFrame(result)

        return signals_metadata

    def generate_personal_id(self, personal_data):
        """Based on the identity, full_name and date of birth."""

        import hashlib

        personal_id = "".join(str(data) for data in personal_data)

        # Remove all whitespaces characters
        personal_id = "".join(personal_id.split())

        # Generate ID
        id = hashlib.sha256(bytes(personal_id, "utf-8")).hexdigest()
        return id

    def upload_file(self, path_to_anonymized_files: str,
                     metadata_file_name, metadata_content) -> None:
        import boto3
        from botocore.client import Config
        import os
        from io import BytesIO

        s3 = boto3.resource('s3',
                            endpoint_url=self.__OBJ_STORAGE_URL__,
                            aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID__,
                            aws_secret_access_key=\
                                self.__OBJ_STORAGE_ACCESS_SECRET__,
                            config=Config(signature_version='s3v4'),
                            region_name=self.__OBJ_STORAGE_REGION__)

        for file in os.listdir(path_to_anonymized_files):
            file_to_upload = os.path.join(path_to_anonymized_files, file)
            if os.path.isfile(file_to_upload):
                s3.Bucket(self.__OBJ_STORAGE_BUCKET__).\
                    upload_file(file_to_upload, "EEGs/edf/" + file)
                if metadata_file_name is not None:
                    obj_name = f"metadata_files/{metadata_file_name}"
                    s3.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_fileobj(
                        BytesIO(metadata_content), obj_name,
                        ExtraArgs={'ContentType': "text/json"})

    def update_filename_pid_mapping(self, obj_name, personal_id, pseudoMRN, mrn):
        import boto3
        from botocore.client import Config
        import csv
        import io

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=\
                                    self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=\
                                    self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)
        folder = "file_pid/"
        filename = "filename_pid.csv"
        file_path = f"{folder}{filename}"
        obj_name_path = f"EEGs/edf/{obj_name}"

        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)
        obj_files = bucket_local.objects.filter(Prefix=folder, Delimiter="/")

        if (len(list(obj_files))) > 0:
            existing_object = s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__,
                                              file_path)
            existing_data = existing_object.get()["Body"].read().decode('utf-8')
            data_to_append = [obj_name_path, personal_id, pseudoMRN, mrn]
            existing_rows = list(csv.reader(io.StringIO(existing_data)))
            existing_rows.append(data_to_append)

            # Update column names
            column_names = ['filename', 'personal_id', 'pseudoMRN', 'MRN']
            if any(col_name not in existing_rows[0] for col_name in column_names):
                existing_rows[0] = column_names

            updated_data = io.StringIO()
            csv.writer(updated_data).writerows(existing_rows)
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(
                io.BytesIO(updated_data.getvalue().encode('utf-8')), file_path)
        else:
            key_values = ['filename', 'personal_id', 'pseudoMRN', 'MRN']
            file_data = [key_values, [obj_name_path, personal_id, pseudoMRN, mrn]]
            updated_data = io.StringIO()
            csv.writer(updated_data).writerows(file_data)
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(
                io.BytesIO(updated_data.getvalue().encode('utf-8')), file_path)

    def calculate_pseudoMRN(self, mrn, workspace_id):
        import hashlib

        if mrn is None:
            pseudoMRN = None
        else:
            personalMRN = [mrn, workspace_id]
            personal_mrn = "".join(str(data) for data in personalMRN)

            # Generate ID
            pseudoMRN = hashlib.sha256(bytes(personal_mrn, "utf-8")).hexdigest()

        return pseudoMRN


    def action(self, input_meta: PluginExchangeMetadata = None) -> \
          PluginActionResponse:
        """
        Run the anonymisation process of the edf files.
        Extract metadata from the edf files.
        Upload anonymized files to the corresponding storage.
        """
        import os
        import shutil
        import pandas as pd

        from trino.dbapi import connect
        from trino.auth import BasicAuthentication

        # Initialize the connection with Trino
        conn = connect(
            host=self.__TRINO_HOST__,
            port=self.__TRINO_PORT__,
            http_scheme="https",
            auth=BasicAuthentication(self.__TRINO_USER__,
                                     self.__TRINO_PASSWORD__)
        )

        # Get the schema name, schema in Trino is an equivalent to a bucket in
        # MinIO Trino doesn't allow to have "-" in schema name so it needs to be
        # replaced with "_"
        schema_name = self.__OBJ_STORAGE_BUCKET__.replace("-", "_")

        # Get the table name
        table_name = self.__OBJ_STORAGE_TABLE__.replace("-", "_")

        path_to_data = \
            "mescobrad_edge/plugins/edf_anonymisation_plugin/anonymize_files/"

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
                    print("Processing started ...")
                    signals, signal_headers, header = \
                        pyedflib.highlevel.read_edf(path_to_file)

                    # Remove personal information from headers
                    remove_values = ["patientname", "birthdate",
                                     "patient_additional", "patientcode",
                                     "admincode", "gender", "sex",
                                     "technician"]

                    # Set empty values
                    new_values = ["", "", "", "", "", "", "", ""]

                    print("Anonymization started ... ")
                    self.anonymize_edf_file(signals, signal_headers, header,
                                            path_to_file,
                                            path_to_anonymized_file,
                                            remove_values, new_values)

                    # Extract metadata information from the edf file, from
                    # signal header
                    list_of_fields_to_extract = ['label','sample_rate',
                                                 'sample_frequency',
                                                 'prefilter', 'dimension']
                    print("Extracting metadata information ... ")
                    data = self.extract_metadata(signal_headers,
                                                 list_of_fields_to_extract)

                    # Extract additional information from header (startdate/time
                    # and duration of the signal)
                    f = pyedflib.EdfReader(path_to_file)
                    file_duration = f.getFileDuration()
                    startdate_time = f.getStartdatetime()
                    del f

                    # Insert additional data in extracted metadata from signal
                    # headers
                    data.insert(0, 'file_duration', file_duration)
                    data.insert(0, 'startdate_time', startdate_time)

                    # Source name of the original edf file
                    source_name = os.path.basename(path_to_file)

                    # Metadata file name
                    metadata_file_template = "{name}.json"
                    if input_meta.data_info["metadata_json_file"] is not None:
                        metadata_file_name = metadata_file_template.format(
                            name=os.path.splitext(source_name)[0])
                    else:
                        metadata_file_name = None

                    # Generate personal id
                    data_info = input_meta.data_info
                    if all(param is not None for param in [data_info['name'],
                                                           data_info['surname'],
                                                           data_info['date_of_birth'],
                                                           data_info['unique_id']]):

                        # Make unified dates, so that different formats of date
                        # doesn't change the final id
                        data_info["date_of_birth"] = pd.to_datetime(
                             data_info["date_of_birth"], dayfirst=True)

                        data_info["date_of_birth"] = \
                            data_info["date_of_birth"].strftime("%d-%m-%Y")

                        # ID is generated base on name, surname, date of birth,
                        # national unique ID
                        personal_data = [data_info['name'],
                                         data_info['surname'],
                                         data_info['date_of_birth'],
                                         data_info['unique_id']]
                        personal_id = self.generate_personal_id(personal_data)
                    else:
                        # TO DO - what is the flow if the data is not provided
                        personal_data = []
                        personal_id = self.generate_personal_id(personal_data)


                    # Insert personal id in the extracted data
                    data.insert(0, "PID", personal_id)

                    # Generate pseudoMRN
                    pseudoMRN = self.calculate_pseudoMRN(
                        input_meta.data_info['MRN'],
                        input_meta.data_info['workspace_id'])

                    # Transform data in suitable form for updating trino table
                    data_transformed = self.transform_input_data(
                        data, source_name, input_meta.data_info["workspace_id"],
                        pseudoMRN, metadata_file_name)

                    self.upload_data_on_trino(schema_name, table_name,
                                              data_transformed, conn)

                    # Update key value file with mapping between filename and
                    # patient id, this file is stored in the local MinIO
                    # instance
                    self.update_filename_pid_mapping(file, personal_id,
                                                     pseudoMRN,
                                                     input_meta.data_info['MRN'])

            # Upload processed data
            print("Uploading file ...")
            self.upload_file(path_to_anonymized_file, metadata_file_name,
                             input_meta.data_info["metadata_json_file"])
            print("Processing of the edf file is finished.")

        except Exception as e:
            print("EDF processing failed with error: " + str(e))

        finally:
            # Remove folder with downloaded and anonymized files
            shutil.rmtree(os.path.split(path_to_anonymized_file)[0])

        return PluginActionResponse()
