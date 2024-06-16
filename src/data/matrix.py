"""Module matrix.py"""
import pandas as pd

import config
import src.data.reference
import src.elements.boundaries
import src.elements.sheet
import src.functions.xlsx


class Matrix:
    """
    Description
    -----------

    Extracts the Emissions & Projects data within the prgrammatically determined 
    boundaries; ref. src.data.boundaries.py.
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
        self.__scope: pd.DataFrame = src.data.reference.Reference().reader(name=self.__configurations.scope)

    def __segment(self, buffer: bytes, boundaries: src.elements.boundaries.Boundaries) -> pd.DataFrame:
        """
        
        :param buffer: A buffer
        :param boundaries: The data boundaries
        :return: A data frame
        """

        self.__dictionary['header'] = 0
        self.__dictionary['skiprows'] =  boundaries.starting
        self.__dictionary['nrows'] = boundaries.ending - boundaries.starting - 2
        sheet = self.__sheet._replace(**self.__dictionary)
        segment: pd.DataFrame = self.__xlsx.decode(buffer=buffer, sheet=sheet)

        return segment

    def __inspect(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param blob: 
        :return: A data frame
        """

        frame: pd.DataFrame = blob.copy().rename(str.lower, axis='columns')
        frame: pd.DataFrame = frame.set_axis(labels=self.__configurations.fields, axis=1)
        frame: pd.DataFrame = frame.copy().loc[
            frame['scope'].str.lower().isin(values=self.__scope['mapping_string'].values), :]

        return frame

    def exc(self, buffer: bytes, metadata: dict, boundaries: src.elements.boundaries.Boundaries):
        """
        :param buffer:
        :param metadata:
        :param boundaries:
        :return: A data frame
        """

        frame: pd.DataFrame = self.__segment(buffer=buffer, boundaries=boundaries)
        frame: pd.DataFrame = self.__inspect(blob=frame)
        frame.dropna(axis=0, subset=['consumption_data'], inplace=True)

        # Markers: These ensure that each record is associated with its start year &
        # organisation identifier
        frame = frame.assign(starting_year=metadata['starting_year'])
        frame = frame.assign(organisation_id=metadata['organisation_id'])

        return frame
