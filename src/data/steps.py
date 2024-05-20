import logging
import os

import pandas as pd

import config
import src.data.interface
import src.data.reference
import src.data.transfer
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.elements.text_attributes as txa
import src.functions.directories
import src.functions.streams


class Steps:

    def __init__(self) -> None:
        """
        Constructor
        """
        
        # Additionally
        self.__reference = src.data.reference.Reference()()
        self.__dictionary: list[dict] = self.__reference.to_dict(orient='records')


        # Logging
        logging.basicConfig(level=logging.INFO,
                        format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                        datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(name=__name__)

    def __get_data(self) -> list:
        """
        :param reference:
        """

        # Execute
        interface = src.data.interface.Interface()
        messages: list = interface.exc(dictionary=self.__dictionary)

        return messages

    def exc(self, hybrid: bool, service: sr.Service = None, s3_parameters: s3p.S3Parameters = None) -> list:
        """

        :param hybrid: Execute cloud & backup programs?  False => Backup only.
        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                              name, buckets, etc.
        :return:
            A list
        """
        
        # Get
        messages: list = self.__get_data()

        # If hybrid, transfer the raw files to Amazon S3 (Simple Storage Service)
        if hybrid:
            transfer = src.data.transfer.Transfer(reference=self.__reference, service=service, s3_parameters=s3_parameters)
            transfers: list[str] = transfer.exc()
            self.__logger.info(msg=transfers)
        
        return messages
