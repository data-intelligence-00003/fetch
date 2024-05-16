"""Module interface.py"""
import os
import dask.delayed
import dask

import config

import src.data.api
import src.elements.s3_parameters as s3p
import src.elements.service as sr

import src.functions.databytes
import src.s3.upload
import src.functions.xlsx


class Interface:
    """
    Interface
    """

    def __init__(self, hybrid: bool, service: sr.Service = None, s3_parameters: s3p.S3Parameters = None) -> None:
        """
        
        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                              name, buckets, etc.
        """
        
        self.__configurations = config.Config()   

        # For Amazon S3
        if hybrid:
            self.__s3_parameters = s3_parameters
            self.__service = service
            self.__upload = src.s3.upload.Upload(service=self.__service, s3_parameters=self.__s3_parameters)  

        # The source's application programming interface instance
        self.__api = src.data.api.API()

        # An instance for fetching and holding in memory
        self.__databytes = src.functions.databytes.DataBytes()
        self.__xlsx = src.functions.xlsx.XLSX()
    
    @dask.delayed
    def __read(self, metadata: dict) -> bytes:
        """
        
        :param metadata: 
        :return:
            A data frame.
        """

        url: str = self.__api.exc(code=metadata['document_id'])            
        buffer: bytes = self.__databytes.get(url=url) 

        return buffer

    @dask.delayed
    def __cloud(self, buffer: bytes, metadata: dict) -> bool:
        """
        
        :param buffer:
        :param metadata:
        :return:
            A boolean indicating data upload success
        """
        
        key_name = f"{self.__s3_parameters.path_internal_raw}{str(metadata['starting_year'])}/{str(metadata['organisation_id'])}.xlsx"
        
        return self.__upload.binary(buffer=buffer, metadata=metadata, key_name=key_name)
    
    @dask.delayed
    def __backup(self, buffer: bytes, metadata: dict) -> bool:
        """
        
        :param buffer:
        :param metadata:
        :return:
            A boolean indicating data upload success
        """

        name: str = os.path.join(self.__configurations.warehouse, str(metadata['starting_year']), str(metadata['organisation_id']))
        
        return self.__xlsx.write(buffer=buffer, name=name)

    def hybrid(self, dictionary: list[dict]):

        computations: list = []
        for metadata in dictionary:
            buffer: bytes = self.__read(metadata=metadata)           
            cloud: bool = self.__cloud(buffer=buffer, metadata=metadata)
            backup: bool = self.__backup(buffer=buffer, metadata=metadata)
            message: str = f"{metadata['organisation_name']}: {cloud}, {backup} ({metadata['starting_year']})"
            computations.append(message)

        messages = dask.compute(computations)
        
        return messages
    
    def single(self, dictionary: list[dict]):

        computations: list = []
        for metadata in dictionary:
            buffer: bytes = self.__read(metadata=metadata)  
            backup: bool = self.__backup(buffer=buffer, metadata=metadata)
            message: str = f"{metadata['organisation_name']}: {backup} ({metadata['starting_year']})"
            computations.append(message)

        messages = dask.compute(computations)

        return messages
