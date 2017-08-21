from src.scheduler import Scheduler
from lib import lib
import pandas as pd

base_config = lib.load_yaml('base_config')

if __name__ == '__main__':
    medic_directory = pd.read_csv(base_config['medic_directory_file_path'])
    scheduler = Scheduler(medic_directory=medic_directory, timezone='US/Central')
    scheduler.run()
