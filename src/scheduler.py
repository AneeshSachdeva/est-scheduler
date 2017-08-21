from twilio.rest import Client
from lib import lib
import pandas as pd
import time, datetime

# Load config params from yaml
twilio_config = lib.load_yaml('twilio_config')
medic_rank_config = lib.load_yaml('medic_rank_config')
shifts_config = lib.load_yaml('shifts_config')

# TODO: should this mapping be in config yaml?
weekday_to_int = {
    'Monday': 0,
    'Tuesday': 1,
    'Wednesday': 2,
    'Thursday': 3,
    'Friday': 4,
    'Saturday': 5,
    'Sunday': 6
}

class Scheduler(object):
    def __init__(self, medic_directory, timezone):
        """
        @param medic_directory: The pandas dataframe containing meta data for all the medics.
        """
        self.medics = medic_directory
        self.timezone_to_utc_offset = self.configure_timezone(timezone) # TODO: see if there's a better way to do this

        # load shift structure from config
        self.shift_times = shifts_config['shift_times']
        self.shift_length_mins = shifts_config['shift_length_mins']
        self.max_signups_per_shift = shifts_config['max_signups_per_shift']

        self.twilio_client = Client(twilio_config['account_sid'], twilio_config['auth_token'])


    def run(self):
        shifts_template = self.initialize_shifts(self.medics, self.shift_times, self.shift_length_mins, self.max_signups_per_shift, self.timezone_to_utc_offset)
        self.schedule(self.medics, shifts_template)


    def schedule(self, medics, shifts_template):
        """
        Schedule shifts. Modfy shifts_config.yaml to change shift structure.
        """
        # Seperate medics by rank and signup priority, and then randomize their order
        priority_to_medics = {}
        for key, value in medic_rank_config['medic_signup_priority'].iteritems():
            # key is signup_priority, value is medic_rank
            # randomize medics by rank
            medics_for_rank = medics[medics['rank'] == value]
            if medics_for_rank.shape[0] > 1:
                medics_for_rank = medics_for_rank.sample(frac=1)
            priority_to_medics[key] = medics_for_rank

        #print(priority_to_medics)


    def initialize_shifts(self, medics, shift_times, shift_length_mins, max_signups_per_shift, timezone_offest):
        """
        Initialize template of shifts to be filled out elsewhere.
        @return: An "empty" pandas dataframe of shifts.
        """
        def find_next_weekday(weekday):
            """
            @param weekday: weekday is 0 based (monday = 0).
            @return: the date of the next occuring weekday. The date will be reset to the start of the day.
            """
            d = datetime.datetime.utcnow() - datetime.timedelta(hours=timezone_offest)
            while d.weekday() != weekday:
                d += datetime.timedelta(days=1)
            return d.replace(hour=0, minute=0, second=0, microsecond=0)

        shifts = []
        num_total_shifts = 0
        iter_shift = None
        for key, value in shift_times.iteritems():
            # key is day, value is array of min hour and max hour
            # number of shifts for current day
            num_shifts = (value[1] - value[0]) * 60 / shift_length_mins # don't convert to float, we want quotient to be floored

            # start with the first hour of the given day
            current_shift = find_next_weekday(weekday_to_int[key]).replace(hour=value[0])
            # iterate through shifts until we reach the max allowed hour for the given day
            for ii in range(num_shifts):
                shifts.append(current_shift)
                current_shift += datetime.timedelta(minutes=shift_length_mins)

            num_total_shifts += num_shifts

        # make sure that we have enough shifts to schedule all medics
        if medics[medics['good_standing'] == True].shape[0] > num_total_shifts * max_signups_per_shift:
            print('ERROR: Not enough shifts to schedule all medics in good standing. Either add shifts or increase max_signups_per_shift.')
            raise # TODO: raise error properly

        # configure the shifts template as a pandas dataframe
        columns = ['shift_time', 'medic_first_name', 'medic_last_name', 'medic_phone_number']
        shift_template = pd.DataFrame(index=range(len(shifts)), columns=columns)

        for ii in range(len(shifts)):
            shift_template.set_value(ii, 'shift_time', shifts[ii])

        return shift_template.sort_values(by='shift_time', ascending=True)


    def alert_medic(self, medic):
        """
        @param medic: A pandas dataframe row containing meta data of a medic.
        """
        message = client.messages.create(
            to=twilio_config['test_phone'],
            from_=twilio_config['est_phone'],
            body='will I ever leave you'
        )

        print(message.sid)


    def configure_timezone(self, zone):
        """
        Find the offset between target timezone and utc.
        """
        # TODO: Handle different timezones
        zone_to_utc_offset = 0

        if zone == 'US/Central':
            zone_to_utc_offset = 5

        return zone_to_utc_offset
