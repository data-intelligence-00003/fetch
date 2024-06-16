"""Module main.py"""
import logging
import os
import sys


def main():
    """
    Entry point
    """

    # Logging
    logger: logging.Logger = logging.getLogger(name=__name__)

    # Executing
    if hybrid:
        _, service, s3_parameters = src.setup.Setup().exc()
        messages = src.data.steps.Steps().exc(hybrid=hybrid, service=service, s3_parameters=s3_parameters)
        logger.info(msg=messages)
    else:
        messages:list = src.data.steps.Steps().exc(hybrid=hybrid)
        logger.info(msg=messages)

    # A simple structure of the extracted emissions data
    src.structuring.simple.Simple().exc()

    # Deleting __pycache__
    src.functions.cache.Cache().exc()


if __name__ == '__main__':
    # Setting-up
    root: str = os.getcwd()
    sys.path.append(root)
    sys.path.append(os.path.join(root, 'src'))

    logging.captureWarnings(capture=True)

    logging.basicConfig(level=logging.INFO,
                        format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Modules
    import src.data.interface
    import src.data.steps
    import src.functions.cache
    import src.functions.service
    import src.s3.s3_parameters
    import src.setup
    import src.structuring.simple

    # Execution
    hybrid = True    

    main()
