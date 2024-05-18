"""Module boundaries.py"""
import typing


class Boundaries(typing.NamedTuple):
    """
    Attributes
    ----------

    starting : int
        The starting row index of an Excel sheet

    ending : int
        The ending row index of an Excel sheet
    """

    starting: int
    ending: int
