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
        
        self.__datapath: str = config.Config().datapath
        self.__streams = src.functions.streams.Streams()
        
        # For Aamazon S3
        self.__s3_parameters = s3_parameters
        self.__upload = src.s3.upload.Upload(service=service, s3_parameters=self.__s3_parameters)

        # The source's application programming interface instance
        self.__api = src.data.api.API()

        # An instance for fetching and holding in memory
        self.__databytes = src.functions.databytes.DataBytes()
    
    def __reference(self, name: str):

        text = txa.TextAttributes(uri=os.path.join(self.__datapath, name), header=0)
        return self.__streams.read(text=text)
    
    def __deliver(self, buffer: bytes, starting_year: int, entity_identifier: int):

        key_name = f'{self.__s3_parameters.path_internal_raw}{str(starting_year)/{str(entity_identifier)}.xlsx}'
        self.__upload.bytes(buffer=buffer, metadata={}, key_name=key_name)

    def exc(self):

        documents: pd.DataFrame = self.__reference(name='documents.csv')
        entities: pd.DataFrame = self.__reference(name='entities.csv')
        reference: pd.DataFrame = documents.merge(entities, how='left', on='entity_identifier').drop(columns=['organisation_code'])
        reference.info()

        for index in reference.index:

            print(reference.iloc[index, :].to_dict())

            url: str = self.__api.exc(code=reference.iloc[index]['document_identifier'])
            print(url)

            # buffer: bytes = self.__databytes.get(url=url)
