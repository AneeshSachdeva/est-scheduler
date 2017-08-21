from twilio.rest import Client
from lib import lib
import pandas as pd
import time, datetime

# Load config params from yaml
twilio_config = lib.load_yaml('twilio_config')
medic_rank_config = lib.load_yaml('medic_rank_config')
shifts_config = lib.load_yaml('shifts_config')

class Scheduler(object):
    def __init__(self, medic_directory):
        """
        @param medic_directory: The pandas dataframe containing meta data for all the medics.
        """
        self.medics = medic_directory

        # load shift structure from config
        self.shift_times = shifts_config['shift_times']
        self.shift_length_mins = shifts_config['shift_length_mins']
        self.max_signups_per_shift = shifts_config['max_signups_per_shift']

        self.shifts = None # initialize this in run()

        self.twilio_client = Client(twilio_config['account_sid'], twilio_config['auth_token'])

        print(medic_rank_config)


    def run(self):
        self.shifts = self.initialize_shifts(self.medics, self.shift_times, self.shift_length_mins, self.max_signups_per_shift)
        self.schedule(self.medics)


    def schedule(self, medics):
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

        print(priority_to_medics)


    def initialize_shifts(self, medics, shift_times, shift_length_mins, max_signups_per_shift):
        num_total_shifts = 0
        for key, value in shift_times.iteritems():
            # key is day, value is array of min hour and max hour
            # don't convert to float, we want quotient to be floored
            num_total_shifts += (value[1] - value[0]) * 60 / shift_length_mins

        # make sure that we have enough shifts to schedule all medics
        if medics[medics['good_standing'] == True].shape[0] > num_total_shifts * max_signups_per_shift:
            print('ERROR: Not enough shifts to schedule all medics in good standing. Either add shifts or increase max_signups_per_shift.')
            raise # TODO: raise error properly

        print(num_total_shifts)


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
