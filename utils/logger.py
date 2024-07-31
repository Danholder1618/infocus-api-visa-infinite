import logging
import os
from datetime import datetime

class ModuleLogger:
    def __init__(self, module_name):
        self.logger = logging.getLogger(module_name)
        self.logger.setLevel(logging.DEBUG)
        log_dir = "./logs"

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_filename = datetime.now().strftime(f"{log_dir}/{module_name}_%Y-%m-%d.log")
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger
