from datetime import datetime, timedelta
from pathlib import Path

def str_to_datetime(date_string):

    if not isinstance(date_string, str):
        print(f'{date_string} is of type {type(date_string)}, not str!')
        return None

    try:
        date_dt = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')
        return date_dt
    except ValueError as e:
        pass
    try: 
        date_dt = datetime.strptime(date_string, '%Y-%m-%d %H:%M')
        return date_dt
    except ValueError as e:
        pass
    try: 
        date_string_stripped = date_string.split('+', 1)[0]
        date_dt = datetime.strptime(date_string_stripped, '%Y-%m-%d %H:%M:%S')
        return date_dt
    except ValueError as e:
        pass
    try: 
        date_string_stripped = date_string.split('.', 1)[0]
        date_dt = datetime.strptime(date_string_stripped, '%Y-%m-%d %H:%M:%S')
        return date_dt
    except ValueError as e:
        pass

    print(f'{date_string} could not be parsed as datetime!')
    return None


def unix_to_date(unix_timestamp):
    if unix_timestamp == 0: 
        return None
    else:
        return datetime.fromtimestamp(int(str(unix_timestamp)[:10]))


def create_sessions(data, interval):
    # timestamp difference in seconds
    diff = data['timestamp'].diff()

    # indexes where new session_id will be created
    new_session = (diff.isnull()) | (diff > timedelta(seconds=interval))

    # Create unique session_id for every user
    data['session_id'] = data.loc[new_session, ['timestamp']].rank(method='first') # .astype(int)
    
    # Propagate last valid observation forward (replace NaN)
    data['session_id'] = data['session_id'].fillna(method='ffill').astype(int)

    return data


def get_features():
    features = ['acousticness', 'danceability', 'energy', 'instrumentalness', 
                'key', 'liveness', 'loudness', 'mode', 'speechiness', 'tempo', 'valence']
    return features


def check_file_exists(path, do_print=True):
    dest = Path(path)
    if not dest.exists():
        if do_print:
            print(path + ' could not be found.')
        return False
    return True