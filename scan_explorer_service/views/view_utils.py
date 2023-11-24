import enum
import os
from adsmutils import setup_logging, load_config

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('view_utils.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'DEBUG'),
                        attach_stdout=config.get('LOG_STDOUT', False))

class ApiErrors(enum.Enum):
    SearchError = 1