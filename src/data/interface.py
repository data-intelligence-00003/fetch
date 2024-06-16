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
    def __backup(self, buffer: bytes, metadata: dict) -> str:
        """
        
        :param buffer:
        :param metadata:
        :return: A str indicating raw data storage success
        """

        name: str = os.path.join(self.__configurations.raw_, 
                                 str(metadata['starting_year']), str(metadata['organisation_id']))
        state: bool = self.__xlsx.write(buffer=buffer, name=name)
        
        return f"Backup -> {state} ({metadata['organisation_name']}, {metadata['starting_year']})"
    
    @dask.delayed
    def __persist(self, blob: pd.DataFrame, metadata: dict) -> str:
        """
        Saves the rough data extract
        
        :param blob:
        :param metadata:
        :return: A str indicating data a successful save action, or otherwise
        """

        name: str = os.path.join(self.__configurations.excerpt_, 
                                 str(metadata['starting_year']), f"{str(metadata['organisation_id'])}.csv")
        message: str = self.__streams.write(blob=blob, path=name)

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
        for metadata in dictionary[:5]:
            url: str = self.__url(metadata=metadata)
            buffer: bytes = databytes(url=url)       
            backup: str = self.__backup(buffer=buffer, metadata=metadata)
            frame: pd.DataFrame = analytics(buffer=buffer, metadata=metadata)
            message = self.__persist(blob=frame, metadata=metadata)   
            computations.append((backup, message))

        messages = dask.compute(computations)[0]
        
        return messages
