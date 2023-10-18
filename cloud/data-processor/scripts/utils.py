import datetime
import pandas

def parse_data(data):
    '''Parses raw data into dataframe with columns [Timestamp, Value]'''
    data = [x.split(":") for x in data]    
    df = read_to_df(data)
    df["Value"] = df["Value"].apply(lambda x:float(x))
    df["Timestamp"] = df["Timestamp"].apply(convert_timestamp)
    return df

def read_to_df(raw_data):
    '''Reads list into dataframe with columns [Timestamp, Value]'''
    df = pandas.DataFrame(raw_data, columns=["Timestamp", "Value"])
    return df

def convert_timestamp(timestamp):
    '''Converts UNIX timestamp to [Year/Month/Second Hour/Minute/Second] format'''
    format = "%Y/%m/%d %H:%M:%S"
    converted = datetime.datetime.fromtimestamp(int(timestamp)).strftime(format)
    print(f"Converted timestamp [{timestamp}] -> [{converted}]", flush=True)
    return converted


