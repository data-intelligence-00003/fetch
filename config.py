import os


class Config:

    def __init__(self) -> None:
        """
        Constructor
        """

        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')

        # A S3 parameters template
        self.s3_parameters_template = 'https://raw.githubusercontent.com/membranes/.github/master/profile/s3_parameters.yaml'
