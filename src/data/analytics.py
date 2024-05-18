"""Module analytics.py"""
import pandas as pd

import src.data.boundaries
import src.data.matrix
import src.elements.boundaries as br


class Analytics:

    def __init__(self) -> None:
        """
        Constructor
        """
        
        self.__boundaries = src.data.boundaries.Boundaries()
        self.__matrix = src.data.matrix.Matrix()

    def exc(self, buffer: bytes, metadata: dict) -> pd.DataFrame:
        """
        
        :param buffer: A buffer of data
        :param metadate: The metadata of the bytes
        :return:
        """

        boundaries: br.Boundaries = self.__boundaries.exc(buffer=buffer)
        matrix: pd.DataFrame = self.__matrix.exc(buffer=buffer, metadata=metadata, boundaries=boundaries)

        return matrix
 