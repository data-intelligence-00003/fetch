"""Module interface.py"""
import os

import dask
import pandas as pd

import config
import src.data.analytics
import src.data.api
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.functions.databytes
import src.functions.xlsx
import src.s3.upload


class Interface:
    """
    Interface
    """

    def __init__(self, hybrid: bool, service: sr.Service = None, s3_parameters: s3p.S3Parameters = None) -> None:
        """
        
        :param hybrid: Execute cloud & backup programs?  False => Backup only.
        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                              name, buckets, etc.
        """
   
        self.__hybrid: bool = False

        # For Amazon S3
        if self.__hybrid:
            self.__s3_parameters = s3_parameters
            self.__service = service
            self.__upload = src.s3.upload.Upload(service=self.__service, s3_parameters=self.__s3_parameters)  

        # In brief (a) an instance of the config.py variables, (b) the source's application programming interface instance, 
        # (c) an instance for fetching data bytes, and holding in memory, (d) an instance for writing, etc., Excel files. 
        self.__configurations = config.Config()
        self.__api = src.data.api.API()
        self.__databytes = src.functions.databytes.DataBytes()
        self.__xlsx = src.functions.xlsx.XLSX()
        
    @dask.delayed
    def __url(self, metadata: dict) -> str:

        return self.__api.exc(code=metadata['document_id'])   

    
    @dask.delayed
    def __read(self, url: str) -> bytes:
        """
        
        :param url: 
        :return: A buffer of data bytes
        """

        buffer: bytes = self.__databytes.get(url=url) 

        return buffer

    @dask.delayed
    def __cloud(self, buffer: bytes, metadata: dict) -> str:
        """
        
        :param buffer:
        :param metadata:
        :return: A str indicating data upload success
        """

        if self.__hybrid:
            key_name = f"{self.__s3_parameters.path_internal_raw}{str(metadata['starting_year'])}/{str(metadata['organisation_id'])}.xlsx"
            state = self.__upload.binary(buffer=buffer, metadata=metadata, key_name=key_name)
            return f"Cloud -> {state} ({metadata['organisation_name']}, {metadata['starting_year']})"
        else:
            return "Cloud -> Skipping"
    
    @dask.delayed
    def __backup(self, buffer: bytes, metadata: dict) -> str:
        """
        
        :param buffer:
        :param metadata:
        :return: A str indicating data upload success
        """

        name: str = os.path.join(self.__configurations.raw_, str(metadata['starting_year']), str(metadata['organisation_id']))
        state: bool = self.__xlsx.write(buffer=buffer, name=name)
        
        return f"Backup -> {state} ({metadata['organisation_name']}, {metadata['starting_year']})"

    def exc(self, dictionary: list[dict]) -> list:

        print(dictionary[:4])

        # Additional delayed tasks
        analytics = dask.delayed(src.data.analytics.Analytics().exc)
        
        # Compute
        computations: list = []
        for metadata in dictionary[:4]:
            url: str = self.__url(metadata=metadata)
            buffer: bytes = self.__read(url=url)           
            cloud: str = self.__cloud(buffer=buffer, metadata=metadata)
            backup: str = self.__backup(buffer=buffer, metadata=metadata)
            matrix: pd.DataFrame = analytics(buffer=buffer, metadata=metadata)    
            computations.append((cloud, backup, matrix))

        messages = dask.compute(computations)[0]
        
        return messages
