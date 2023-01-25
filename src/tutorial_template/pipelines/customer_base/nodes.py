"""
This is a boilerplate pipeline 'customer_base'
generated using Kedro 0.18.4
"""
from typing import Dict
import logging

from tutorial_template.pipelines.utils.redshift import Redshift
from tutorial_template.pipelines.utils.sql_formatter import Formatter

from kedro.config import ConfigLoader
from kedro.framework.project import settings


def run_sql_queries(data, sql_parameters: Dict, redshift: Dict):
    """This function runs a function to perform aggregations on redshift data and save to s3
    """
    log = logging.getLogger(__name__)

    # Parameterize
    script_string = Formatter.insert_vars(
        data,
        **sql_parameters
    )

    statements = Formatter.split_statements(script_string)

    # Execute
    conf_path = str(settings.CONF_SOURCE)
    conf_loader = ConfigLoader(conf_source=conf_path, env="local")
    conf_credentials = conf_loader.get("credentials*", "credentials*/**")

    redshift_secrets = conf_credentials['redshift']

    connector = Redshift(**redshift_secrets, **redshift)

    log.info('Generating customer base')
    results = connector.execute_multiple(statements)

    log.info('Customer base')