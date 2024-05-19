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
        
    def __reference(self, name: str) -> pd.DataFrame:
        """
        
        :param name: The name of a CSV reference file within the project's data directory; including the file's extension.
        :return:
            A data frame.
        """

        text = txa.TextAttributes(
            uri=os.path.join(self.__configurations.datapath, name), header=0)

        return self.__streams.read(text=text)
    
    def __types(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param blob:
        :return: A frame
        """

        emission_types: pd.DataFrame = self.__reference(name=self.__configurations.emission_types)

        # Mapping strings
        frame = blob.assign(mapping_string=blob['emission_type'].str.lower())
        frame.drop(columns='emission_type', inplace=True)

        # Missing mapping string
        condition: pd.Series[bool] = frame['mapping_string'].isin(values=emission_types['mapping_string'].values)
        frame.loc[~condition, 'mapping_string'] = 'other'

        # Emission type identification codes
        frame: pd.DataFrame = frame.copy().merge(right=emission_types, how='left', on='mapping_string')
        
        return frame.drop(columns='mapping_string')

    def __sources(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param blob:
        :return: A frame
        """

        emission_sources: pd.DataFrame = self.__reference(name=self.__configurations.emission_sources)

        # Mapping strings
        frame: pd.DataFrame = blob.assign(mapping_string=blob['emission_source'].str.lower())
        frame.drop(columns='emission_source', inplace=True)

        # Emission source identification codes
        frame = frame.copy().merge(right=emission_sources, how='left', on=['emission_type_id', 'mapping_string'])
        
        return frame.drop(columns='mapping_string')
    
    def __units(self, blob: pd.DataFrame):
        """
        
        :param blob:
        :return: A frame
        """

        units: pd.DataFrame = self.__reference(name=self.__configurations.units)

        # Mapping strings
        frame: pd.DataFrame = blob.assign(mapping_string=blob['consumption_data_unit'].str.lower())
        frame.drop(columns='consumption_data_unit', inplace=True)

        # Identification codes
        frame = frame.copy().merge(right=units, how='left', on='mapping_string')
        frame.drop(columns=['mapping_string', 'description'], inplace=True)
        
        return frame.rename(columns={'unit_of_measure': 'consumption_data_unit'})

    def exc(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param blob:
        :return:
            A data frame
        """
        
        data: pd.DataFrame = blob.copy()
        data = self.__types(blob=data)
        data = self.__sources(blob=data)
        data = self.__units(blob=data)

        return data
