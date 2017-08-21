# Note: This class isn't currently being used, but might be useful later.
class Medic(object):
    def __init__(self, first_name, last_name, phone_number, rank, good_standing=True):
        self.first_name = first_name
        self.last_name = last_name
        self.phone_number = phone_number
        self.rank = rank
        self.good_standing = good_standing
