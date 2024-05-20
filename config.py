
import os


class Config:

    def __init__(self) -> None:
        """
        Constructor
        """

        # Warehouse
        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.raw_: str = os.path.join(self.warehouse, 'raw')
        self.excerpt_: str = os.path.join(self.warehouse, 'excerpt')
        self.structures_: str = os.path.join(self.warehouse, 'structures')
        self.sections: list[str] = [self.raw_, self.excerpt_, self.structures_]

        # Data
        self.datapath: str = os.path.join(os.getcwd(), 'data')

        # Fields
        self.fields: list[str] = ['emission_type', 'emission_source', 'scope', 'consumption_data', 'consumption_data_unit', 
                       'emission_factor', 'emission_factor_unit', 'emission_tCO2e', 'comment']
        
        # Structuring
        self.exclude: list[str] = ['emission_type', 'emission_source', 'scope', 'consumption_data_unit']


        # The documents of interest, and the names of their source organisations
        self.documents = 'documents.csv'
        self.organisations = 'organisations.csv'
        self.emission_types = 'emission_types.csv'
        self.emission_sources = 'emission_sources.csv'
        self.units = 'units.csv'
        self.scope = 'scope.csv'

        # A S3 parameters template
        self.s3_parameters_template = 'https://raw.githubusercontent.com/prml-0003/.github/master/profile/s3_parameters.yaml'


