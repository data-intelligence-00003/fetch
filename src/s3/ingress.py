"""
Module ingress.py
"""

import botocore.exceptions

import src.elements.s3_parameters as s3p
import src.elements.service as sr


class Ingress:
    """
    Class Ingress

    Description
    -----------

    Uploads files to Amazon Simple Storage Service (S3)
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters:
        """

        self.__service = service
        self.__s3_parameters = s3_parameters

    def exc(self, file: str, key: str, metadata: dict) -> str:
        """

        :param file: The local file string, i.e., <path> + <file name> + <extension>, of the file being uploaded
        :param key: The Amazon S3 key of the file being uploaded; the key is relative-to the S3 Bucket name, but excludes
                    the S3 Bucket name.
        :param metadata: 
        :return:
            A message string
        """

        rebuilt: dict = {key: str(value) for key, value in metadata.items()}

        try:
            self.__service.s3_client.upload_file(Filename=file, Bucket=self.__s3_parameters.internal, Key=key,
                                                 ExtraArgs={'Metadata': rebuilt})
            return f'Uploading {key}'
        except botocore.exceptions.ClientError as err:
            raise err from err
