"""Module boundaries.py"""
import pandas as pd

import src.elements.sheet
import src.elements.boundaries
import src.functions.xlsx


class Boundaries:
    """
    Description
    -----------

    Determines the limits within which the Emissions & Projects data lies.
    """

    def __init__(self) -> None:
        """
        Constructor
        """

        # An instance for reading data cells
        self.__xlsx = src.functions.xlsx.XLSX()

        # The attributes of the Excel Sheet in focus
        self.__dictionary: dict = {'sheet_name': 'Emissions and Projects', 
                 'header': None, 'skiprows': 0, 'usecols': 'D', 'nrows': None}
        self.__sheet = src.elements.sheet.Sheet()

    def __segment(self, url: str) -> pd.DataFrame:
        """
        
        :param url: A data file's uniform resource locator
        :return:
            A data frame
        """

        # Setting the final set of Excel Sheet attributes
        self.__dictionary['io'] = url        
        sheet = self.__sheet._replace(**self.__dictionary)

        # Reading the data cells
        segment: pd.DataFrame = self.__xlsx.read(sheet=sheet)

        return segment
    
    def __inspect(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param blob: A single field data frame; refer to self.__dictionary above.
        :return:
            A data frame
        """

        # This section renames the single field, and drops empty cells
        frame = blob.copy().set_axis(labels=['source'], axis=1)
        frame = frame.assign(source=frame['source'].str.lower())
        frame: pd.DataFrame = frame.copy().dropna(axis=0)

        return frame
    
    def __starting(self, blob: pd.DataFrame) -> int:
        """
        
        :param blob:
        :return:
            A data frame
        """        

        frame: pd.DataFrame = blob.copy()
        index: int = frame.index[frame['source'].str.lower() == 'emission source'].values[0] 
        
        return index
    
    def __ending(self, blob: pd.DataFrame) -> int:
        """
        
        :param blob:
        :return:
            A data frame
        """

        frame: pd.DataFrame = blob.copy()
        index: int = frame.index[frame['source'].str.lower() == 'total consumed by the body (kwh)'].values[0] 
        
        return index

        
    def exc(self, url: str) -> src.elements.boundaries.Boundaries:
        """
        
        :param url: A document's url
        :return:
            A data frame
        """

        segment: pd.DataFrame = self.__segment(url=url)
        segment: pd.DataFrame = self.__inspect(blob=segment)
        starting: int = self.__starting(blob=segment)
        ending: int = self.__ending(blob=segment)

        boundaries = src.elements.boundaries.Boundaries(starting=starting, ending=ending)

        return boundaries
