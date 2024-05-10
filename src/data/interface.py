import os
import pandas as pd

import config

import src.data.api
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.elements.text_attributes as txa
import src.functions.databytes
import src.functions.streams
import src.s3.upload


class Interface:

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters) -> None:
        
        self.__configurations = config.Config()
        self.__streams = src.functions.streams.Streams()
        
        # For Amazon S3
        self.__s3_parameters = s3_parameters
        self.__upload = src.s3.upload.Upload(service=service, s3_parameters=self.__s3_parameters)

        # The source's application programming interface instance
        self.__api = src.data.api.API()

        # An instance for fetching and holding in memory
        self.__databytes = src.functions.databytes.DataBytes()
    
    def __reference(self, name: str) -> pd.DataFrame:
        """
        
        :param name: The name of a data file within the project's data directory; including the file's extension.
        :return:
            A data frame.
        """

        text = txa.TextAttributes(uri=os.path.join(self.__configurations.datapath, name), header=0)

        return self.__streams.read(text=text)
    
    def __deliver(self, buffer: bytes, metadata: dict) -> bool:
        """
        
        :param buffer:
        :param metadata:
        :return:
            A boolean indicating data upload success
        """
        
        key_name = f"{self.__s3_parameters.path_internal_raw}{str(metadata['starting_year'])}/{str(metadata['entity_identifier'])}.xlsx"

        return self.__upload.binary(buffer=buffer, metadata=metadata, key_name=key_name)

    def exc(self) -> list:
        """
        
        :return:
            A list
        """

        documents: pd.DataFrame = self.__reference(name='documents.csv')
        organisations: pd.DataFrame = self.__reference(name='organisations.csv')
        reference: pd.DataFrame = documents.merge(organisations, how='left', on='organisation_id').drop(columns=['organisation_type_id'])
        reference.info()

        computations = []
        for index in reference.index[:5]:

            metadata: dict = reference.iloc[index, :].to_dict()

            url: str = self.__api.exc(code=metadata['document_id'])            
            buffer: bytes = self.__databytes.get(url=url) 
            
            message: bool = self.__deliver(buffer=buffer, metadata=metadata)
            computations.append(f"{metadata['organisation_name']}: {message} ({metadata['starting_year']})")
        
        return computations
