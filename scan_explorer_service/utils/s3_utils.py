import io
from flask import current_app
import boto3
from botocore.exceptions import ClientError, ParamValidationError


class S3Provider:
    """
    Class for interacting with a particular S3 provider
    """

    def __init__(self, config, bucket_name):
        """
        input:

        config: 
        """
        
        self.s3 = boto3.resource("s3")
        self.bucket = self.s3.Bucket(config.get(bucket_name))


    def write_object_s3(self, file_bytes, object_name):
        try:
            response = self.bucket.put_object(Body=file_bytes, Key=object_name)
        except (ClientError, ParamValidationError) as e:
            current_app.logger.info.exception(e)
            raise e
        return response.e_tag

    def read_object_s3(self, object_name):
        try:
            with io.BytesIO() as s3_obj:
                self.bucket.download_fileobj(object_name, s3_obj)
                s3_obj.seek(0)
                s3_file = s3_obj.read()
                return s3_file
        except (ClientError, ParamValidationError) as e:
            current_app.logger.exception(e)
            raise e


