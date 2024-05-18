
import typing

class Sheet(typing.NamedTuple):

    io: str = ''
    sheet_name: str = None
    header: int = None
    skiprows: int = 0
    usecols: str = None
    nrows: int = None
