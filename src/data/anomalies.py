import os

import pandas as pd

import config
import src.elements.text_attributes as txa
import src.functions.streams


class Anomalies:

    def __init__(self) -> None:
        """
        Constructor
        """
        
        self.__configurations = config.Config()
        self.__streams = src.functions.streams.Streams()

        # The numeric identifiers of emission types
        emission_types: pd.DataFrame = self.__reference(name=self.__configurations.emission_types)
        self.__emission_types: pd.DataFrame = emission_types.assign(
            emission_type=emission_types['emission_type'].str.lower())
        
        emission_sources = self.__reference(name=self.__configurations.emission_sources)
        self.__emission_sources = emission_sources.assign(
            emission_source_mapping_string=emission_sources['emission_source_mapping_string'].str.lower())

    def __reference(self, name: str) -> pd.DataFrame:
        """
        
        :param name: The name of a CSV reference file within the project's data directory; including the file's extension.
        :return:
            A data frame.
        """

        text = txa.TextAttributes(
            uri=os.path.join(self.__configurations.datapath, name), header=0)

        return self.__streams.read(text=text)


    def exc(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param blob:
        :return:
            A data frame
        """
        
        print(self.__emission_types)

        # Types
        frame = blob.assign(emission_type=blob['emission_type'].str.lower())
        condition: pd.Series[bool] = frame['emission_type'].isin(values=self.__emission_types['emission_type'].values)
        frame.loc[~condition, 'emission_type'] = 'other'
        frame: pd.DataFrame = frame.copy().merge(right=self.__emission_types, how='left', on='emission_type')

        # Sources
        data: pd.DataFrame = frame.assign(emission_source_mapping_string=frame['emission_source'].str.lower())
        data.drop(columns='emission_source', inplace=True)
        data = data.copy().merge(self.__emission_sources, how='left', on=['emission_type_id', 'emission_source_mapping_string'])
        data.drop(columns='emission_source_mapping_string', inplace=True)

        return data
