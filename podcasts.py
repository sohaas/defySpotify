"""
TODO check categories for each podcast manually in Spotify app
"""

import pandas as pd
from os import listdir

from utils.utils import check_file_exists
from utils.api_token import get_api_token
from utils.api_requests import get_api_podcasts
from utils.parse_data import read_endsong


def get_podcasts(subject="001"):

    path = 'data/' + subject + '/MyData 2/'
    df_path = 'output/' + subject + '/podcasts.csv'

    if check_file_exists(df_path, False):
        print('Podcast file already exists for subject ' + subject)
        return
    else:
        print('Retrieving podcasts for subject ' + subject)

    files = [path + x for x in listdir(path)
             if x.split('.')[0][:-1] == 'endsong_']
    streamings = read_endsong(path, len(files))

    episodes = streamings[~streamings['episode_name'].isnull()]
    print(f'Discovered {len(episodes)} episodes.')

    cols = episodes.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    episodes = episodes[cols]

    episodes = episodes[['timestamp', 'ms_played', 'episode_name', 'episode_show_name', 'episode_uri', 'reason_start', 'reason_end']]

    token = get_api_token('user-read-playback-position')

    extras = []
    acquired = 0
    #TODO this is also ugly
    empty = {'description': None,
             'duration_ms': None, 
             'language': None, 
             'languages': None, 
             'release_date': None, 
             'show_description': None,
             'show_publisher': None}
    for index, episode in episodes.iterrows():
        show_id = episode['episode_uri']
        show_id = show_id.split('spotify:episode:')[1]
        try:
            extra_info = get_api_podcasts(show_id, token)
            if extra_info: 
                extras.append(extra_info)
                acquired += 1
            else:
                extras.append(empty)
        except:
            extras.append(empty)
            
        if acquired % 250 == 0 and acquired != 0:
            print(f'Already recovered information about {acquired} podcasts.')
        # handle access token expire
        if acquired % 5000 == 0:
            token = get_api_token('user-read-playback-position')

    print(f'Successfully recovered information about {acquired} podcasts in total.')
    if len(episodes) - acquired != 0:
        print(f'Failed to identify {len(episodes) - acquired} items.')

    extra_df = pd.DataFrame.from_records(extras)
    extra_df = extra_df.reset_index(drop=True)
    episodes = episodes.reset_index(drop=True)
    joined_df = pd.concat([episodes, extra_df], axis=1)
    joined_df.to_csv(df_path)


if __name__ == "__main__":
    get_podcasts("001")
