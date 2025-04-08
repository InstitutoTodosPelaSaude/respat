import io
from pathlib import Path
import pandas as pd
from datetime import datetime
import os
import zipfile

from minio import Minio
from minio.error import S3Error
from minio.commonconfig import REPLACE, CopySource

class FileSystem():
    _instance = None

    # [WIP] Make this class a singleton
    def __init__(self, root_path: str, secure=False):
        """
        Wrapper to interact with the application files stored in MinIO.
        HOW THE VARIABLE ROOT_PATH WORKS?
            It must be a string in the following format: /bucketname/my/root/path
            The first part of the string is the BUCKET NAME in MinIO
            The rest is the base path, from which all the interactions will be made relative.
            
            For example, if you want to delete the object stored in the bucket LOCUS with path /my/loved/loveLetter.txt 
            You CAN instantiate the filesystem = FileSystem(root_path='/locus/my/') and call filesystem.delete_file('/loved/loveLetter.txt').

        Args:
            root_path (str): Root path. /bucketname/path
        """
        self.root_path  = root_path
        self.endpoint   = os.getenv("MINIO_ENDPOINT")
        self.access_key = os.getenv("MINIO_ACCESS_KEY")
        self.secret_key = os.getenv("MINIO_SECRET_KEY")
        self.secure = secure
        self.client = None

        self.connect()


    def connect(self):
        """
        Establishes a connection to the MinIO server using the provided environment variables.
        """
        try:
            # Create a MinIO client
            self.client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )

            # Check if the bucket exists or can be accessed
            bucket_name = self.root_path.split("/")[1]  # Assuming root_path is in form /bucketname/path
            if not self.client.bucket_exists(bucket_name):
                raise S3Error(f"Bucket '{bucket_name}' does not exist or is not accessible.")

            print(f"Connected successfully to MinIO bucket: {bucket_name}")
            self.bucket_name = bucket_name
            self.root_path = "/".join( self.root_path.split("/")[2:] )
        except S3Error as e:
            self.bucket_name = None
            print(f"Error connecting to MinIO: {str(e)}")


    def list_files_in_relative_path(self, relative_path: str, accepted_extensions=None, recursive=False):
    
        try:
            prefix = self.root_path+relative_path
            print(prefix)

            # List objects in the specified path (prefix)
            objects = self.client.list_objects(self.bucket_name, prefix=str(prefix), recursive=recursive)

            # Filter files by extension if provided
            file_list = []
            for obj in objects:
                if accepted_extensions:
                    # Check if the object's extension matches the accepted extensions
                    if any(obj.object_name.endswith(ext) for ext in accepted_extensions):
                        file_list.append(obj.object_name)
                else:
                    file_list.append(obj.object_name)

            return file_list
        except S3Error as e:
            print(f"Error listing files in MinIO: {e}")
            

    def save_content_in_file(self, relative_path, content, file_name, log_context = None, content_type = "application/octet-stream"):
        try:
            if not relative_path.endswith("/"):
                relative_path += "/"
                
            object_name = self.root_path + relative_path + file_name
            object_name = str(object_name).replace("//", "/")

            self.client.put_object(
                self.bucket_name,
                object_name,
                io.BytesIO(content),
                length=len(content),
                content_type=content_type
            )
            
            return True
        except Exception as e:
            print(f'Error saving content in file: {e}')
            if log_context:
                log_context.error(f'Error saving content in file: {e}')
            return False


    def get_file_content_as_io_bytes(self, relative_path):
        try:
            object_path = self.root_path + relative_path
            response = self.client.get_object(self.bucket_name, object_path)
            return io.BytesIO(response.read())
        except Exception as e:
            print(f'Error getting file content as io bytes: {e}')
            return None


    def get_file_content_as_binary(self, relative_path):

        try:
            # Get the object as a stream
            response = self.client.get_object(self.bucket_name, self.root_path +relative_path)
            
            # Read the object content as binary data
            binary_data = response.read()
            
            response.close()
            response.release_conn()

            return binary_data
        
        except S3Error as e:
            print(f"Error occurred while fetching the file: {e}")
            return f'Error reading file: {e}'.encode('utf-8')
        
        finally:
            response.close()
            response.release_conn()


    def get_file_last_modified_date(self, relative_path):

        try:
            file_stats = self.client.stat_object(self.bucket_name, self.root_path+relative_path)
            last_modified_timestamp = file_stats.last_modified

            # Remove Time zone info to make it a 'Offset-Naive' timestamp, just like datetime.now()
            last_modified_timestamp = last_modified_timestamp.replace(tzinfo=None)

            return last_modified_timestamp
        
        except S3Error as e:
            return datetime.now()


    def read_all_files_in_folder_as_buffer(self, relative_path, accepted_extensions=None):
        files = self.list_files_in_relative_path(relative_path, accepted_extensions)
        files.sort()

        dt_now               = datetime.now()
        dt_last_modification = lambda file:self.get_file_last_modified_date(file)

        try:

            # file.replace(self.root_path, '', 1) removes the root path from the 'file' path
            # because the methods get_file_content_as_binary and get_file_creation_date
            # works only with relative paths
            
            file_contents = []
            for file in files:
                file = file.replace(self.root_path, '', 1)
                file_contents.append((
                    file, 
                    self.get_file_content_as_binary(file),
                    (dt_now - dt_last_modification(file)).total_seconds() # Time in seconds since last modification
                ))

        except Exception as e:
            file_contents = [
                (f'UNABLE TO READ FILE {e}', f'Error reading file: {e}'.encode('utf-8'), 0)
                for file in files
            ]

        return file_contents
    

    def get_path_contents_as_zip_file(self, relative_path, accepted_extensions):
        file_content_list = self.read_all_files_in_folder_as_buffer(relative_path, accepted_extensions)

        zip_file_name = f"{relative_path.replace('/', '')}.zip"
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for filename, file_contents, _ in file_content_list:
                filename = str(filename).split('/')[-1]
                zip_file.writestr(filename, file_contents)

        return zip_buffer.getvalue(), zip_file_name
    

    def move_file_to_folder(self, relative_path, file_name, target_folder):
        try:
            original_file_absolute_path  = self.root_path + relative_path + file_name
            target_file_path             = self.root_path + target_folder + file_name

            self.client.copy_object(
                self.bucket_name, 
                target_file_path, 
                CopySource( self.bucket_name, original_file_absolute_path )
            )

            # Remove the original file
            self.client.remove_object(self.bucket_name, original_file_absolute_path)

            return True
        except S3Error as e:
            print(f'Error moving file to folder: {e}')
            return False
        

    def copy_file_to_folder(self, relative_path, file_name, target_folder):
        try:
            original_file_absolute_path  = self.root_path + relative_path + file_name
            target_file_path             = self.root_path + target_folder + file_name

            self.client.copy_object(
                self.bucket_name, 
                target_file_path, 
                CopySource( self.bucket_name, original_file_absolute_path )
            )

            return True
        except S3Error as e:
            print(f'Error copying file to folder: {e}')
            return False

   
    def delete_file(self, relative_path):
        absolute_object_path = self.root_path + relative_path

        try:
            self.client.remove_object(self.bucket_name, absolute_object_path)
            return True
        except S3Error as e:
            print(f'Error deleting file: {e}')
            return False
        
