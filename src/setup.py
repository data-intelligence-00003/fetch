"""
Module setup.py
"""

import typing

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.s3_parameters
import src.functions.service
import src.s3.bucket


class Setup:
    """

    Notes
    -----

    This class prepares the Amazon S3 (Simple Storage Service) and local data environments.
    """

    def __init__(self):
        """
        
        
        """

        # S3 S3Parameters Instance, Service Instance
        self.__s3_parameters: s3p.S3Parameters = src.s3.s3_parameters.S3Parameters().exc()
        self.__service: sr.Service = src.functions.service.Service(region_name=self.__s3_parameters.region_name).exc()

    def __s3(self) -> bool:
        """
        Prepares an Amazon S3 (Simple Storage Service) bucket.

        :return:
        """

        # An instance for interacting with Amazon S3 buckets.
        bucket = src.s3.bucket.Bucket(service=self.__service, location_constraint=self.__s3_parameters.location_constraint,
                                      bucket_name=self.__s3_parameters.internal)

        if bucket.exists():
            return bucket.empty()

        return bucket.create()

    def exc(self) -> typing.Tuple[bool, sr.Service, s3p.S3Parameters]:
        """

        :return:
            :setup:
            :service: A suite of services for interacting with Amazon Web Services.
            :s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                            name, buckets, etc.
        """

        setup: bool = self.__s3()

        return setup, self.__service, self.__s3_parameters
