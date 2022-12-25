from mescobrad_edge.plugins.edf_anonymisation_plugin.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata

class GenericPlugin(EmptyPlugin):
    def download_file(self, file_path: str) -> None:
        import boto3
        from botocore.client import Config
        import os

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        s3 = boto3.resource("s3",
                            endpoint_url=self.__OBJ_STORAGE_URL__,
                            aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID__,
                            aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET__,
                            config=Config(signature_version='s3v4'),
                            region_name=self.__OBJ_STORAGE_REGION__)

        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)
        bucket = s3.Bucket(self.__OBJ_STORAGE_BUCKET__)
        # Existing non annonymized data in local MinIO bucket
        obj_personal_data = bucket_local.objects.filter(Prefix="edf_data/", Delimiter="/")
        # Anonymized data in remote MinIO bucket
        obj_anonymous_data = bucket.objects.filter(Prefix="edf_anonymized_data/", Delimiter="/")

        keys_anonymous_data = [os.path.basename(obj.key) for obj in obj_anonymous_data]
        # Files for anonymization
        files_to_anonymize = [obj.key for obj in obj_personal_data if os.path.basename(obj.key) not in keys_anonymous_data]

        # Download data which need to be anonymized
        for file_name in files_to_anonymize:
            path_download_file = file_path+os.path.basename(file_name)
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).download_file(file_name, path_download_file)

    def anonymize_edf_file(self, edf_file, new_file_path, to_remove, new_values):
        """Anonymize edf file by removing values of personal data in header fields."""
        import pyedflib
        import os

        if not len(to_remove) == len(new_values):
            raise AssertionError('Each to_remove must have one new_value')

        # Get all the data from the original file
        signals, signal_headers, header = pyedflib.highlevel.read_edf(edf_file)

        # Remove personal information from header
        for new_val, attr in zip(new_values, to_remove):
            if attr in header.keys():
                header[attr] = new_val

        # Write new file with the same content and anonymized header
        new_file_name = new_file_path + "/" + os.path.basename(edf_file)
        pyedflib.highlevel.write_edf(new_file_name, signals, signal_headers, header)

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
                s3.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_file(file_to_upload, "edf_anonymized_data/" + file)

    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        """
        Run the anonymisation process of the edf files.
        """
        import os
        import shutil

        path_to_data = "mescobrad_edge/plugins/edf_anonymisation_plugin/anonymize_files/"

        # create temporary folder for storing downloaded files
        os.makedirs(path_to_data, exist_ok=True)

        # Download data to process
        self.download_file(path_to_data)

        # Anonymize and upload anonymized files
        path_to_anonymized_file = path_to_data + "anonymized"
        os.makedirs(path_to_anonymized_file, exist_ok=True)

        for file in os.listdir(path_to_data):
            path_to_file = os.path.join(path_to_data, file)
            if os.path.isfile(path_to_file):
                # Remove personal information from headers
                remove_values = ["patientname", "birthdate", "patient_additional"] # to do add more or remove values if needed
                new_values = ["", "", ""]
                self.anonymize_edf_file(path_to_file, path_to_anonymized_file, remove_values, new_values)

        # Upload processed data
        self.upload_file(path_to_anonymized_file)

        # Remove folder with downloaded and anonymized files
        shutil.rmtree(os.path.split(path_to_anonymized_file)[0])

        print("Anonymization of the edf file is finished.")

        return PluginActionResponse()
