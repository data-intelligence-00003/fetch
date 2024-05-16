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
        setup, service, s3_parameters = src.setup.Setup().exc()
        messages = src.data.steps.Steps(service=service, s3_parameters=s3_parameters).exc(hybrid=hybrid)
        logger.info(msg=messages)
    else:
        logger.info('Private')
        messages = src.data.steps.Steps().exc(hybrid=hybrid)
        logger.info(msg=messages)

    # Deleting __pycache__
    src.functions.cache.Cache().exc()


if __name__ == '__main__':
    # Setting-up
    root = os.getcwd()
    sys.path.append(root)
    sys.path.append(os.path.join(root, 'src'))

    logging.basicConfig(level=logging.INFO,
                        format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Modules
    import src.data.interface
    import src.data.steps
    import src.elements.s3_parameters as s3p
    import src.elements.service as sr
    import src.functions.cache
    import src.functions.service
    import src.s3.s3_parameters
    import src.setup

    # Execution
    hybrid = False    

    main()
