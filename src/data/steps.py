import os
import pandas as pd

import config
import src.functions.directories
import src.functions.streams
import src.elements.text_attributes as txa
import src.data.interface
import src.elements.s3_parameters as s3p
import src.elements.service as sr

class Steps:

    def __init__(self) -> None:
        """
        Constructor
        """
        
        # Additionally
        self.__configurations = config.Config()
        self.__streams = src.functions.streams.Streams()

    def __reference(self, name: str) -> pd.DataFrame:
        """
        
        :param name: The name of a CSV data file within the project's data directory; including the file's extension.
        :return:
            A data frame.
        """

        text = txa.TextAttributes(uri=os.path.join(self.__configurations.datapath, name), header=0)

        return self.__streams.read(text=text)
    
    def __directories(self, dictionary: list[dict]) -> list[bool]:

        years: list[str] = [str(metadata['starting_year']) for metadata in dictionary]
        directories = src.functions.directories.Directories()
        directories.cleanup(path=self.__configurations.warehouse)
        states: list[bool] = [directories.create(path=os.path.join(self.__configurations.warehouse, year)) 
                  for year in years]
        
        return states

    def exc(self, hybrid: bool, service: sr.Service = None, s3_parameters: s3p.S3Parameters = None) -> list:
        """

        :param hybrid: Execute cloud & backup programs?
        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                              name, buckets, etc.
        :return:
            A list
        """

        # References
        documents: pd.DataFrame = self.__reference(name=self.__configurations.documents)
        organisations: pd.DataFrame = self.__reference(name=self.__configurations.organisations)
        reference: pd.DataFrame = documents.merge(organisations, how='left', on='organisation_id').drop(columns=['organisation_type_id'])
        dictionary = reference.to_dict(orient='records')

        # Backup Directories
        self.__directories(dictionary=dictionary)

        # Execute
        if hybrid:
            interface = src.data.interface.Interface(hybrid=True, service=service, s3_parameters=s3_parameters)
            return interface.hybrid(dictionary=dictionary)
        else:
            interface = src.data.interface.Interface(hybrid=False)
            return interface.single(dictionary=dictionary)
