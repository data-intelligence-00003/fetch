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

        self.__scope = config.Config().scope

    def __segment(self, url: str, boundaries: src.elements.boundaries.Boundaries) -> pd.DataFrame:
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

        segment: pd.DataFrame = self.__xlsx.read(sheet=sheet)

        return segment
    
    def __inspect(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param blob: 
        :return:
            A data frame
        """

        
        frame = blob.copy().rename(mapper=str.lower, axis=1)
        frame = frame.copy().loc[frame['scope'].isin(self.__scope), :]
        # frame.dropna(axis=0, subset=['scope'], inplace=True)

        return frame

    def exc(self, url: str, boundaries: src.elements.boundaries.Boundaries):

        frame = self.__segment(url=url, boundaries=boundaries)
        frame = self.__inspect(blob=frame)

        return frame