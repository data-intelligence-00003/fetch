"""Module simple.py"""
import glob
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

        # An instance for reading CSV files
        self.__streams = src.functions.streams.Streams()

    def __get_data(self, uri: str) -> pd.DataFrame:
        """
        
        :param uri:
        :return: A data frame
        """

        text = txa.TextAttributes(uri=uri, header=0)

        return self.__streams.read(text=text)
    
    def __exclude(self, blob: pd.DataFrame) -> pd.DataFrame:
        """
        Excludes text identifiers
        
        :param blob:
        :return: A data frame
        """
  
        frame: pd.DataFrame = blob.copy().drop(columns=self.__configurations.exclude, axis=1)

        return frame

    def exc(self):
        """
        
        """

        paths: list[str] = glob.glob(pathname=os.path.join(self.__configurations.excerpt_, '**', '*.csv' ))

        computations = []
        for path in paths:
            data: pd.DataFrame = self.__get_data(uri=path)
            data = self.__exclude(blob=data)
            
            computations.append(data)

        frame = pd.concat(computations, axis=0, ignore_index=True)
        
        message = self.__streams.write(blob=frame, path=os.path.join(self.__configurations.structures_, 'simple.csv'))

        print(message)
