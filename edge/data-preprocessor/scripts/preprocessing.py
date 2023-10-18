from datetime import datetime as dt

class Preprocessor():
    '''
    Class to perform stream-preprocessing calculations for 
    incoming data from data-injector
    '''
    def __init__(self):
        '''Initialises variables'''
        self.DAY_LOWER_BOUND = None
        self.DAY_UPPER_BOUND = None
        self.DAY_VALUES = []

    def process_value(self, x):
        '''
        Stream-processes values individually
        Collects values by week and processes them when next week value
        comes in.
        If x=finished then process any remaining values
        ASSUMES MESSAGES ARE IN CHRONOLOGICAL ORDER

        Attributes:
            self: object values
            x: individual message in timestamp:value format

        Returns: 
            None: if value is just added to DAY_VALUES
            (int): Previous week average
        '''
        # When finished process remaining values
        if x == "finished" and len(self.DAY_VALUES) > 0:
            return self.batch_preprocess(self.DAY_VALUES)

        # Parse received value
        timestamp, value = x.split(":")
        # Convert from string to float
        value = float(value)

        # Convert from string and change to seconds
        timestamp = int(timestamp) / 1000

        # Check for outlier
        if value > 50:
            print(f"OUTLIER: {x}", flush=True)
            return False
        
        # If lower bound/upper bound not initialised
        if self.DAY_LOWER_BOUND is None or self.DAY_UPPER_BOUND is None:
            # Set day bounds in unix time
            self.DAY_LOWER_BOUND = self.get_day_low_bound(timestamp)
            # Add one day to upper bound to lower bound
            self.DAY_UPPER_BOUND = self.DAY_LOWER_BOUND + (24*60*60)
            self.DAY_VALUES = []

        # If value is within day bounds
        if self.DAY_LOWER_BOUND <= timestamp and timestamp < self.DAY_UPPER_BOUND:
            # Add value todays list
            self.DAY_VALUES.append(value)
            return None 
        else:
            # If timestamp is after current day
            if timestamp >= self.DAY_UPPER_BOUND:
                # A value from a new day has arrived
                # Process data from previous day and return with start timestamp
                rtn = f"{int(self.DAY_LOWER_BOUND)}:{self.batch_preprocess(self.DAY_VALUES)}"

                # Reset bounds and array around new value
                self.DAY_LOWER_BOUND = self.get_day_low_bound(timestamp)
                # Add one day to upper bound to lower bound
                self.DAY_UPPER_BOUND = self.DAY_LOWER_BOUND + (24*60*60)
                # Reset day values and include current value
                self.DAY_VALUES = [value]

                return rtn

            # NOTE
            # If value is before the current day it is ignored
            # Values are assumed to be in chronological order
    

    def get_day_low_bound(self, unix_time):
        '''Get UNIX midnight timestamp of unix_time value'''
        # Get year/month/day of utc time 
        utc_time = dt.fromtimestamp(unix_time).replace(
            hour=0,\
            minute=0,\
            second=0
            )
        
        # Convert back to unix time
        return utc_time.timestamp()
        
    def batch_preprocess(self, x):
        '''Gets average value of list'''
        return sum(x)/len(x)
    


    

