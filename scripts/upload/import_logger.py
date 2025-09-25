import logging.config
from logging import Logger


def get_logger(log_level: object = "INFO") -> Logger:
    """
    Configure and create a logger for the upload scripts

    Args:
        log_level: Log level to use

    Returns:
        The configured logger
    """
    logging.config.dictConfig(
        {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'simple': {
                    'format': '%(asctime)s - %(levelname)s - %(message)s'
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                },
            },
            'loggers': {
                'ImportLogger': {
                    'level': log_level,
                    'handlers': ['console'],
                    'propagate': False,
                },
            },
        }

    )

    return logging.getLogger('ImportLogger')
