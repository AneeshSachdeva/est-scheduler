from twilio.rest import Client
from lib import lib
import pandas as pd
import time, datetime, threading

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


    def run(self, output_file_path):
        """
        Main loop of the scheduler.
        @param output_file_path: The file path for the filled out shift schedule as a csv.
        """
        shifts_template = self.initialize_shifts(self.medics,
                                                self.shift_times,
                                                self.shift_length_mins,
                                                self.max_signups_per_shift,
                                                self.timezone_to_utc_offset)
        schedule = self.schedule(self.medics, shifts_template)
        schedule.to_csv('{}.csv'.format(output_file_path)) # write schedule to human readable file

        # schedule messaging
        itr = 2 # for testing
        for shift_time in schedule['shift_time_utc'].unique():
            print(shift_time)
            medics_for_shift = schedule[(schedule['shift_time_utc'] == shift_time)
                                        & (pd.isnull(schedule).any(axis=1) == False)]
            now = datetime.datetime.utcnow()
            seconds_till_send = (shift_time - now).total_seconds()
            t = threading.Timer(seconds_till_send, self.message_medics, [], {'medics': medics_for_shift})
            t.daemon = False # keep thread alive after main closes
            t.start()
            itr += 1


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

        # TODO: Fill out shifts_template with priority_to_medics
        shift_idx = 0
        for key, value in priority_to_medics.iteritems():
            # key is priority, value is pandas df of medics for that priority
            if value.shape[0] > 0:
                for idx, row in value.iterrows():
                    # each row is a medic
                    if row['good_standing'] is True:
                        shifts_template.set_value(shift_idx, 'medic_first_name', row['first_name'])
                        shifts_template.set_value(shift_idx, 'medic_last_name', row['last_name'])
                        shifts_template.set_value(shift_idx, 'medic_phone_number', row['phone_number'])
                        shift_idx += 1
                for idx, row in value[::-1].iterrows():
                    # reverse the order
                    if row['good_standing'] is True:
                        shifts_template.set_value(shift_idx, 'medic_first_name', row['first_name'])
                        shifts_template.set_value(shift_idx, 'medic_last_name', row['last_name'])
                        shifts_template.set_value(shift_idx, 'medic_phone_number', row['phone_number'])
                        shift_idx += 1

        print(shifts_template)
        return shifts_template # this is now a schedule, not just a template


    def initialize_shifts(self, medics, shift_times, shift_length_mins, max_signups_per_shift, timezone_offest):
        """
        Initialize template of shifts to be filled out elsewhere.
        @return: An "empty" pandas dataframe of shifts, sorted by ascending signup time.
        """
        def find_next_weekday(weekday):
            """
            @param weekday: Weekday is 0 based (monday = 0).
            @return: The date of the next occuring weekday. The date will be reset to the start of the day.
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
        # TODO: parameterize signups per medic (currently 2)
        if medics[medics['good_standing'] == True].shape[0] * 2 / max_signups_per_shift > num_total_shifts * max_signups_per_shift:
            print(medics[medics['good_standing'] == True].shape[0] * 2 / max_signups_per_shift)
            print(num_total_shifts)
            print('ERROR: Not enough shifts to schedule all medics in good standing. Either add shifts or increase max_signups_per_shift.')
            raise # TODO: raise error properly

        # configure the shifts template as a pandas dataframe
        columns = ['shift_time_utc', 'shift_time_local', 'medic_first_name', 'medic_last_name', 'medic_phone_number']
        shift_template = pd.DataFrame(index=range(len(shifts) * max_signups_per_shift), columns=columns)

        idx = 0
        for shift_time in shifts:
            for jj in range(max_signups_per_shift):
                shift_template.set_value(idx + jj, 'shift_time_local', shift_time)
                shift_template.set_value(idx + jj, 'shift_time_utc', shift_time + datetime.timedelta(hours=self.timezone_to_utc_offset))
            idx += max_signups_per_shift

        return shift_template.sort_values(by='shift_time_local', ascending=True).reset_index(drop=True)


    def send_message(self, sender, recipient, body):
        """
        @param medic: A pandas dataframe row containing meta data of a medic.
        """
        message = self.twilio_client.messages.create(
            to=recipient,
            from_=sender,
            body=body
        )

        print(datetime.datetime.utcnow(), message.sid)


    def message_medics(self, *args, **kwargs):
        medics_to_message = kwargs['medics']
        for idx, row in medics_to_message.iterrows():
            if row['medic_phone_number']:
                message_body = 'Hey narc, time to sign for a shift. You have %d minutes. \n http://est.wustl.edu/shift_scheduler' % self.shift_length_mins
                self.send_message(sender=twilio_config['est_phone'], recipient=row['medic_phone_number'], body=message_body)


    def configure_timezone(self, zone):
        """
        Find the offset between target timezone and utc.
        """
        # TODO: Handle different timezones
        zone_to_utc_offset = 0

        if zone == 'US/Central':
            zone_to_utc_offset = 5

        return zone_to_utc_offset
