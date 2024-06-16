"""
Module setup.py
"""

import itertools
import os
import typing

import pandas as pd

import config
import src.data.reference
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.functions.directories
import src.functions.service
import src.s3.bucket
import src.s3.s3_parameters


class Setup:
    """

    Notes
    -----

    This class prepares the Amazon S3 (Simple Storage Service) and local data environments.
    """

    def __init__(self):
        """
        Constructor        
        """

        reference = src.data.reference.Reference()
        self.__dictionary: list[dict] = reference().to_dict(orient='records')

        # Instances
        self.__directories = src.functions.directories.Directories()

        # Configurations
        self.__configurations = config.Config()

        # S3 S3Parameters Instance, Service Instance
        self.__s3_parameters: s3p.S3Parameters = src.s3.s3_parameters.S3Parameters().exc()
        self.__service: sr.Service = src.functions.service.Service(region_name=self.__s3_parameters.region_name).exc()

    def __backup(self) -> bool:
        """
        Option: assert sum(cases) == len(cases)

        :param dictionary:
        """

        years: list[str] = [str(metadata['starting_year']) for metadata in self.__dictionary]
        self.__directories.cleanup(path=self.__configurations.warehouse)

        # Raw & Excerpts
        sections = []
        for section in self.__configurations.sections:
            states: list[bool] = [self.__directories.create(path=os.path.join(section, year)) for year in years]
            sections.append(states)

        cases = list(itertools.chain(*sections))
        
        return all(cases)

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
            setup: Did the set-up succeed?
            service: A suite of services for interacting with Amazon Web Services.
            s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                           name, buckets, etc.
        """

        s3: bool = self.__s3()
        backup: bool = self.__backup()
        setup: bool = s3 & backup

        return setup, self.__service, self.__s3_parameters
