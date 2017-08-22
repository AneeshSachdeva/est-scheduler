from src.scheduler import Scheduler
from lib import lib
import pandas as pd
import datetime

base_config = lib.load_yaml('base_config')

if __name__ == '__main__':
    medic_directory = pd.read_csv(base_config['medic_directory_file_path'])
    scheduler = Scheduler(medic_directory=medic_directory, timezone='US/Central')
    output_file_name = 'test'
    scheduler.run(output_file_path='{}/{}'.format(base_config['output_directory'], output_file_name))
    # test_time = datetime.datetime(2017, 8, 22, 8, 17)
    # now_time = datetime.datetime.utcnow()
    # print(test_time.time(), datetime.time(now_time.month, now_time.hour, now_time.minute))
    # scheduler.alert_medic(medic=None, signup_time=test_time)
