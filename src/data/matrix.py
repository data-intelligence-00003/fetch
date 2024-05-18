"""Module matrix.py"""
import pandas as pd

import config
import src.elements.boundaries
import src.elements.sheet
import src.functions.xlsx


class Matrix:
    """
    Description
    -----------

    Extracts the Emissions & Projects data within suggested limits.
    """

    def __init__(self) -> None:
        """
        Constructor
        """

        self.__xlsx = src.functions.xlsx.XLSX()
        self.__dictionary: dict = {'sheet_name': 'Emissions and Projects', 'usecols': 'C:K'}
        self.__sheet = src.elements.sheet.Sheet()

        # Configurations
        self.__configurations = config.Config()

    def __segment(self, url: str, buffer: bytes, boundaries: src.elements.boundaries.Boundaries) -> pd.DataFrame:
        """
        
        :param url: A data file's uniform resource locator
        :param boundaries: The data boundaries
        :return:
            A data frame
        """

        self.__dictionary['io'] = url
        self.__dictionary['header'] = 0
        self.__dictionary['skiprows'] =  boundaries.starting
        self.__dictionary['nrows'] = boundaries.ending - boundaries.starting - 2
        sheet = self.__sheet._replace(**self.__dictionary)

        # segment: pd.DataFrame = self.__xlsx.read(sheet=sheet)
        segment: pd.DataFrame = self.__xlsx.decode(buffer=buffer, sheet=sheet)

        return segment
    
    def __inspect(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        frame.dropna(axis=0, subset=['scope'], inplace=True)
        
        :param blob: 
        :return:
            A data frame
        """

        
        frame: pd.DataFrame = blob.copy().rename(mapper=str.lower, axis=1)
        frame: pd.DataFrame = frame.set_axis(labels=self.__configurations.fields, axis=1)
        frame: pd.DataFrame = frame.copy().loc[frame['scope'].isin(self.__configurations.scope), :]

        return frame

    def exc(self, url: str, buffer: bytes, metadata: dict, boundaries: src.elements.boundaries.Boundaries):

        frame: pd.DataFrame = self.__segment(url=url, buffer=buffer, boundaries=boundaries)
        frame: pd.DataFrame = self.__inspect(blob=frame)
        frame = frame.assign(starting_year=metadata['starting_year'])
        frame = frame.assign(organisation_id=metadata['organisation_id'])

        return frame