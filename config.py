import os


class Config:

    def __init__(self) -> None:
        """
        Constructor
        """

        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.datapath: str = os.path.join(os.getcwd(), 'data')

        # Scope
        self.scope: list[str] = ['Scope 1', 'Scope 2', 'Scope 3', 'Combined scopes (for EVs only)']

        # The documents of interest, and the names of their source organisations
        self.documents = 'documents.csv'
        self.organisations = 'organisations.csv'

        # A S3 parameters template
        self.s3_parameters_template = 'https://raw.githubusercontent.com/data-intelligence-00003/.github/master/profile/s3_parameters.yaml'
