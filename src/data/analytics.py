"""Module analytics.py"""
import pandas as pd

import src.anomalies.emissions as es
import src.anomalies.scope as se
import src.anomalies.units as us
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
        frame: pd.DataFrame = es.Emissions().exc(blob=matrix)
        frame: pd.DataFrame = se.Scope().exc(blob=frame)
        frame: pd.DataFrame = us.Units().exc(blob=frame)

        return frame
 