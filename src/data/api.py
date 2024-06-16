"""Module api.py"""
import logging


class API:
    """
    Class API
    """

    def __init__(self) -> None:
        """
        Constructor
        """

        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def __pattern(self, code: str) -> str:
        """

        :param code: The document's code
        :return:
        """

        string = f"""https://sustainablescotlandnetwork.org/slickr_media_upload?id={code}"""

        self.__logger.info(string)

        return string

    def exc(self, code: str) -> str:
        """

        :param code: A document's code
        :return:
        """

        return self.__pattern(code=code)
