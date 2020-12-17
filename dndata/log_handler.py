from dnscript.common.logging_manager import LoggingManager
import logging

log_output_dir = 'log'
logging_manager = LoggingManager(log_output_dir)

logging_option = {
    'system': {
        'is_make_file': True
    },
    'system.error': {
        'is_make_file': True
    },
}


def get_logger(name) -> logging.Logger:
    option = logging_option.get(name, None)
    if option is None:
        return None

    return logging_manager.get_logger(name, option.get('is_make_file', False))
