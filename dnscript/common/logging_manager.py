import os
import logging
from logging import handlers


class LoggingManager:
    output_dir: str = None
    logger_dict: dict = {}

    def __init__(self, output_dir):
        self.output_dir = output_dir
        if output_dir is not None:
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

    def add_logger(self, name, is_make_file=True):
        logger = logging.getLogger(name)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        logger.addHandler(stream_handler)

        if self.output_dir is not None and is_make_file:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s')
            filename = os.path.join(self.output_dir, name)
            file_handler = handlers.TimedRotatingFileHandler(filename=f'{filename}.log', when='midnight', interval=1,
                                                             encoding='utf-8')
            file_handler.setFormatter(formatter)
            file_handler.suffix = "%Y%m%d"
            file_handler.setLevel(logging.INFO)

            logger.addHandler(file_handler)

        logger.setLevel(logging.DEBUG)

        self.logger_dict[name] = logger

        return logger

    def get_logger(self, name, is_make_file):
        logger = self.logger_dict.get(name, None)
        if logger is None:
            logger = self.add_logger(name, is_make_file)

        return logger
