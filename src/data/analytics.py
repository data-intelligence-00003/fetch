
import pandas as pd

import src.data.boundaries
import src.data.matrix
import src.elements.boundaries as br


class Analytics:

    def __init__(self) -> None:
        
        self.__boundaries = src.data.boundaries.Boundaries()
        self.__matrix = src.data.matrix.Matrix()

    def exc(self, url: str) -> pd.DataFrame:
        """
        
        :param url:
        :return:
        """

        boundaries: br.Boundaries = self.__boundaries.exc(url=url)
        matrix: pd.DataFrame = self.__matrix.exc(url=url, boundaries=boundaries)

        return matrix
 