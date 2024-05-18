
import pandas as pd

import src.data.boundaries
import src.data.matrix
import src.elements.boundaries as br


class Analytics:

    def __init__(self) -> None:
        
        self.__boundaries = src.data.boundaries.Boundaries()
        self.__matrix = src.data.matrix.Matrix()

    def exc(self, url: str, buffer: bytes, metadata: dict) -> pd.DataFrame:
        """
        
        :param url:
        :return:
        """

        boundaries: br.Boundaries = self.__boundaries.exc(url=url, buffer=buffer)
        matrix: pd.DataFrame = self.__matrix.exc(url=url, buffer=buffer, metadata=metadata, boundaries=boundaries)

        return matrix
 