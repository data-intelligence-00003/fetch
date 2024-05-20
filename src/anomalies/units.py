"""Module units.py"""
import pandas as pd

import config
import src.data.reference


class Units:
    """
    Units
    """

    def __init__(self) -> None:
        """
        Constructor
        """
        
        self.__configurations = config.Config()
        self.__reference = src.data.reference.Reference()

    def __units(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param blob:
        :return: A frame
        """

        units: pd.DataFrame = self.__reference.reader(name=self.__configurations.units)

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
                A data frame that includes the numeric identifier of a unit of measure
            """

            return self.__units(blob=blob.copy())
