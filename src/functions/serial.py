"""
Module serial.py
"""
import yaml

import src.functions.databytes


class Serial:
    """
    Class Serial

    Description
    -----------
    Present, this class reads-in local YAML data files; YAML is a data serialisation language.
    """

    def __init__(self):
        """
        Constructor
        """

        self.__databytes = src.functions.databytes.DataBytes()

    def api(self, url: str) -> dict:
        """

        :param url: The uniform resource locator (URL) of a data set.
        :return:
        """

        response = self.__databytes.content(url=url)

        if response.status_code == 200:
            content = response.content.decode(encoding='utf-8')
            return yaml.safe_load(content)
        raise f'Failure code: {response.status_code}'

    @staticmethod
    def read(uri: str) -> dict:
        """

        :param uri: The file string of a local YAML file; path + file name + extension.
        :return:
        """

        with open(file=uri, mode='r', encoding='utf-8') as stream:
            try:
                return yaml.load(stream=stream, Loader=yaml.CLoader)
            except yaml.YAMLError as err:
                raise err from err
