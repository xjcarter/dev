import os
import glob
import archive_utils
from datetime import datetime, timedelta

PORTFOLIO_DIRECTORY = os.environ.get('PORTFOLIO_DIRECTORY', '/home/jcarter/junk/portfolio/')

# Specify the directories to monitor
# path = /home/portfolio/<StrategyName>/logs
log_directories = glob.glob(f'{PORTFOLIO_DIRECTORY}/*/logs')
before_dt = datetime.now() - timedelta(days=5)
archive_utils.compress_files_in_directories(log_directories, file_tag=".log", before=before_dt )
