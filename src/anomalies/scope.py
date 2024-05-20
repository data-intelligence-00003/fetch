"""Module scope.py"""
import pandas as pd

import config
import src.data.reference


class Scope:
    """
    Scope
    """

    def __init__(self) -> None:
        """
        Constructor
        """
        
        self.__configurations = config.Config()
        self.__reference = src.data.reference.Reference()

    def __scope(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param blob:
        :return: A frame
        """

        units: pd.DataFrame = self.__reference.reader(name=self.__configurations.scope)

        # Mapping strings
        frame: pd.DataFrame = blob.assign(mapping_string=blob['scope'].str.lower())
        frame.drop(columns='scope', inplace=True)

        # Identification codes
        frame = frame.copy().merge(right=units, how='left', on='mapping_string')
        frame.drop(columns='mapping_string', inplace=True)
        
        return frame
    
    def exc(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
            
            :param blob:
            :return:
                A data frame that includes the numeric identifier of a scope type
            """

        return self.__scope(blob=blob.copy())
