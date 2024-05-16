"""
Module acq.py
"""
import requests

class DataBytes:

    def __init__(self) -> None:
        pass

    @staticmethod
    def get(url: str) -> bytes:
        """

        :param url:
        :return:
        """

        try:
            response: requests.Response = requests.get(url=url, timeout=600)
            response.raise_for_status()
        except requests.exceptions.Timeout as err:
            raise err from err
        except Exception as err:
            raise err from err

        if response.status_code == 200:
            content: bytes = response.content
            return content
        raise f'Failure code: {response.status_code}'
