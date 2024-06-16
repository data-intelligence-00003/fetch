"""Module emission.py"""
import pandas as pd

import config
import src.data.reference


class Emissions:
    """
    Emissions
    """

    def __init__(self) -> None:
        """
        Constructor
        """

        self.__configurations = config.Config()
        self.__reference = src.data.reference.Reference()

    def __types(self, blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob:
        :return: A frame
        """

        emission_types: pd.DataFrame = self.__reference.reader(name=self.__configurations.emission_types)

        # Mapping strings
        frame = blob.assign(mapping_string=blob['emission_type'].str.lower())
        frame.drop(columns='emission_type', inplace=True)

        # Missing mapping string
        condition: pd.Series = frame['mapping_string'].isin(values=emission_types['mapping_string'].values)
        frame.loc[~condition, 'mapping_string'] = 'other'

        # Emission type identification codes
        frame: pd.DataFrame = frame.copy().merge(right=emission_types, how='left', on='mapping_string')

        return frame.drop(columns='mapping_string')

    def __sources(self, blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob:
        :return: A frame
        """

        emission_sources: pd.DataFrame = self.__reference.reader(name=self.__configurations.emission_sources)

        # Mapping strings
        frame: pd.DataFrame = blob.assign(mapping_string=blob['emission_source'].str.lower())
        frame.drop(columns='emission_source', inplace=True)

        # Emission source identification codes
        frame = frame.copy().merge(right=emission_sources, how='left', on=['emission_type_id', 'mapping_string'])

        return frame.drop(columns='mapping_string')

    def exc(self, blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob:
        :return:
            A data frame that includes the numeric identifier (1) of emission types, and (2) of the emission
            source of an emission type
        """

        data: pd.DataFrame = blob.copy()
        data = self.__types(blob=data)
        data = self.__sources(blob=data)

        return data
