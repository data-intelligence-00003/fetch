"""Module interface.py"""
import os

import dask
import dask.delayed
import pandas as pd

import config
import src.data.analytics
import src.data.api
import src.functions.databytes
import src.functions.streams
import src.functions.xlsx
import src.s3.upload


class Interface:
    """
    Interface
    """

    def __init__(self) -> None:
        """
        
        Constructor
        """

        # In brief (a) an instance of the config.py variables, (b) the source's application programming interface instance, 
        # (c) an instance for fetching data bytes, and holding in memory, (d) an instance for writing, etc., Excel files. 
        self.__configurations = config.Config()
        self.__api = src.data.api.API()
        self.__databytes = src.functions.databytes.DataBytes()
        self.__xlsx = src.functions.xlsx.XLSX()
        self.__streams = src.functions.streams.Streams()
        
    @dask.delayed
    def __url(self, metadata: dict) -> str:
        """
        
        :param metadata:
        :return:
        """

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
    def __backup(self, buffer: bytes, metadata: dict) -> str:
        """
        
        :param buffer:
        :param metadata:
        :return: A str indicating data upload success
        """

        name: str = os.path.join(self.__configurations.raw_, str(metadata['starting_year']), str(metadata['organisation_id']))
        state: bool = self.__xlsx.write(buffer=buffer, name=name)
        
        return f"Backup -> {state} ({metadata['organisation_name']}, {metadata['starting_year']})"
    
    @dask.delayed
    def __persist(self, blob: pd.DataFrame, metadata: dict) -> str:
        """
        
        :param blob:
        :param metadata:
        :return: A str indicating data a successful save action, or otherwise
        """

        name: str = os.path.join(self.__configurations.excerpt__, str(metadata['starting_year']), str(metadata['organisation_id']))
        message = self.__streams.write(blob=blob, path=name)

        return message

    def exc(self, dictionary: list[dict]) -> list:
        """
        
        :param dictionary:
        """

        # Additional delayed tasks
        databytes = dask.delayed(src.functions.databytes.DataBytes().get)
        analytics = dask.delayed(src.data.analytics.Analytics().exc)
        
        # Compute
        computations: list = []
        for metadata in dictionary[:4]:
            url: str = self.__url(metadata=metadata)
            # buffer: bytes = self.__read(url=url)
            buffer: bytes = databytes(url=url)       
            backup: str = self.__backup(buffer=buffer, metadata=metadata)
            frame: pd.DataFrame = analytics(buffer=buffer, metadata=metadata)    
            computations.append((backup, frame))

        messages = dask.compute(computations)[0]
        
        return messages
