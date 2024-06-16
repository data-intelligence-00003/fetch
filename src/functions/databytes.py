"""
Module acq.py
"""
import requests


class DataBytes:
    """
    Retrieve requests content
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def content(url) -> requests.Response:
        """

        :param url: The uniform resource locator (URL) of a data set.
        :return:
        """

        try:
            response: requests.Response = requests.get(url=url, timeout=600)
            response.raise_for_status()
        except requests.exceptions.Timeout as err:
            raise err from err
        except Exception as err:
            raise err from err

        return response

    def get(self, url: str) -> bytes:
        """

        :param url: The uniform resource locator (URL) of a data set.
        :return:
        """

        response = self.content(url=url)

        if response.status_code == 200:
            content: bytes = response.content
            return content
        raise f'Failure code: {response.status_code}'
