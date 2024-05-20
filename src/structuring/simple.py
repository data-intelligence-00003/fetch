"""Module simple.py"""
import glob
import logging
import os

import pandas as pd

import config
import src.elements.text_attributes as txa
import src.functions.streams


class Simple:
    """
    Simple

    A simple standard structure under development
    """

    def __init__(self) -> None:
        """
        Constructor
        """
        
        self.__configurations = config.Config()

        self.__select: list[str] = ['consumption_data', 'consumption_data_unit_id', 'emission_factor', 'emission_factor_unit', 
                                    'emission_tCO2e', 'starting_year', 'organisation_id', 'emission_type_id', 'emission_source_id', 
                                    'scope_id', 'comment']

        # An instance for reading CSV files
        self.__streams = src.functions.streams.Streams()

        # Logging
        logging.basicConfig(level=logging.INFO,
                        format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                        datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger: logging.Logger = logging.getLogger(name=__name__)

    def __get_data(self, uri: str) -> pd.DataFrame:
        """
        
        :param uri:
        :return: A data frame
        """

        text = txa.TextAttributes(uri=uri, header=0)

        return self.__streams.read(text=text)

    def exc(self):
        """
        A simple structure for modelling & analysis
        """

        paths: list[str] = glob.glob(pathname=os.path.join(self.__configurations.excerpt_, '**', '*.csv' ))

        # Compute
        computations = []
        for path in paths:
            data: pd.DataFrame = self.__get_data(uri=path)
            data = data.copy()[self.__select]            
            computations.append(data)

        # Concatenate frames
        frame: pd.DataFrame = pd.concat(computations, axis=0, ignore_index=True)
        
        # Save
        message: str = self.__streams.write(
            blob=frame, path=os.path.join(self.__configurations.structures_, 'simple.csv'))

        self.__logger.info(msg=message)
