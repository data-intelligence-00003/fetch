"""Module transfer.py"""
import glob
import os
import pathlib
import typing

import dask
import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.ingress
import config


class Transfer:
    """
    Transfer
    """

    def __init__(self, reference: pd.DataFrame, service: sr.Service, s3_parameters: s3p.S3Parameters) -> None:
        """
        
        :param reference:
        :param services:
        :param s3_parameters:
        """
        
        self.__reference: pd.DataFrame = reference
        self.__configurations = config.Config()

        # The list of files to be uploaded
        self.__files: list[str] = glob.glob(
            pathname=os.path.join(self.__configurations.raw_, '**', '*.xlsx'), recursive=True)

        # An instance of Ingress
        self.__s3_parameters = s3_parameters
        self.__ingress = src.s3.ingress.Ingress(service=service, s3_parameters=self.__s3_parameters)

    def __tags(self) -> typing.Tuple[list[str], list[str], list[str]]:
        """
        Tags of a data file
        """
        
        names: list[str] =  [os.path.basename(file) for file in self.__files]        
        years: list[int] = [int(os.path.basename(os.path.dirname(file))) for file in self.__files]
        keys: list[str] = [f'{self.__s3_parameters.path_internal_raw}{year}/{name}' for year, name in zip(years, names)]

        return names, years, keys
    
    def __dictionary(self, names: list[str], years: list[int]) -> list[dict]:
        """
        
        :param names:
        :param years:
        """

        identifiers: list[int] = [int(pathlib.PurePath(name).stem) for name in names]
        lines = pd.DataFrame(data={'organisation_id': identifiers, 'starting_year': years})
        reference: pd.DataFrame = self.__reference.copy().merge(right=lines, how='right', on=['organisation_id', 'starting_year'])
        dictionary: list[dict] = reference.to_dict(orient='records')

        return dictionary

    def exc(self) -> list[str]:
        """
        The metadata of the files being uploaded. Note, files of the same content type are expected, assumed.

        :return:
        """

        names, years, keys = self.__tags()
        dictionary = self.__dictionary(names=names, years=years)

        # Compute
        ingress = dask.delayed(self.__ingress.exc)
        computations = []
        for file, key, metadata in zip(self.__files, keys, dictionary):
            message = ingress(file=file, key=key, metadata=metadata)
            computations.append(message)
        
        messages = dask.compute(computations, scheduler='threads')[0]

        return messages
