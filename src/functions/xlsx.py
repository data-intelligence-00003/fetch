import io

import pandas as pd

import src.elements.sheet


class XLSX:
    """
    Excel: .xlsx
    """

    def __init__(self) -> None:
        pass

    # noinspection PyTypeChecker
    @staticmethod
    def decode(buffer: bytes, sheet: src.elements.sheet.Sheet) -> pd.DataFrame:
        """
        
        :param buffer:
        :param sheet: @ src.elements.sheet.Sheet
        """

        try:
            return pd.read_excel(io=io.BytesIO(initial_bytes=buffer), sheet_name=sheet.sheet_name,
                                 header=sheet.header, skiprows=sheet.skiprows, nrows=sheet.nrows,
                                 usecols=sheet.usecols)
        except OSError as err:
            raise Exception(err.strerror) from err

    # noinspection PyTypeChecker
    @staticmethod
    def read(sheet: src.elements.sheet.Sheet) -> pd.DataFrame:
        """
        
        :param sheet: @ src.elements.sheet.Sheet
        """

        try:
            return pd.read_excel(io=sheet.io, sheet_name=sheet.sheet_name, header=sheet.header,
                                 skiprows=sheet.skiprows, nrows=sheet.nrows, usecols=sheet.usecols,
                                 engine='openpyxl')
        except OSError as err:
            raise Exception(err.strerror) from err

    @staticmethod
    def write(buffer: bytes, name: str) -> bool:
        """
        
        :param buffer:
        :param name:
        """

        try:
            with open(f"{name}.xlsx", "wb") as file:
                file.write(buffer)
            return True
        except BufferError as err:
            raise err from err
        except Exception as err:
            raise err from err
