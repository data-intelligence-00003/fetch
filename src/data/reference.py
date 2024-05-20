"""Module reference.py"""
import os

import pandas as pd

import config
import src.elements.text_attributes as txa
import src.functions.streams


class Reference:

    def __init__(self):
        """
        Constructor
        """

        self.__configurations = config.Config()

        # An instance for interacting with CSV files
        self.__streams = src.functions.streams.Streams()
        
        # Get the inventory/metadata of climate documents & health organisations
        documents: pd.DataFrame = self.reader(name=self.__configurations.documents)
        organisations: pd.DataFrame = self.reader(name=self.__configurations.organisations)

        # The frame of reference data
        self.reference: pd.DataFrame = documents.merge(
            right=organisations, how='left', on='organisation_id').drop(columns=['organisation_type_id'])

    def reader(self, name: str) -> pd.DataFrame:
        """
        
        :param name: The name of a CSV reference file within the project's data directory; including the file's extension.
        :return:
            A data frame.
        """

        text = txa.TextAttributes(
            uri=os.path.join(self.__configurations.datapath, name), header=0)

        return self.__streams.read(text=text)

    def __call__(self) -> pd.DataFrame:
        """
        :return:
            A frame of references
        """    

        return self.reference
