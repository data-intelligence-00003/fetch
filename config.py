import os


class Config:

    def __init__(self) -> None:
        """
        Constructor
        """

        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.raw_: str = os.path.join(self.warehouse, 'raw')
        self.excerpt_: str = os.path.join(self.warehouse, 'excerpt')
        self.datapath: str = os.path.join(os.getcwd(), 'data')

        # Scope
        self.scope: list[str] = ['Scope 1', 'Scope 2', 'Scope 3', 'Combined scopes (for EVs only)']

        # Fields
        self.fields: list[str] = ['emission_type', 'emission_source', 'scope', 'consumption_data', 'consumption_data_units', 
                       'emission_factor', 'emission_factor_units', 'emissions', 'comment']

        # The documents of interest, and the names of their source organisations
        self.documents = 'documents.csv'
        self.organisations = 'organisations.csv'
        self.emission_types = 'emission_types.csv'
        self.emission_sources = 'emission_sources.csv'

        # A S3 parameters template
        self.s3_parameters_template = 'https://raw.githubusercontent.com/data-intelligence-00003/.github/master/profile/s3_parameters.yaml'
