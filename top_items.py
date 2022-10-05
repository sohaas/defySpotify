"""
Unfortunately Spotify is an asshole and I can only request this data for myself :)

TODO can I find out how often I streamed things ?
"""

import pandas as pd

from utils.api_token import get_api_token
from utils.api_requests import get_api_top_items


def get_top_items():

    df_path_tracks = 'output/top_tracks.csv'
    df_path_artists = 'output/top_artists.csv'

    token = get_api_token('user-top-read')

    print('Retrieving top tracks for myself.')
    top_tracks = get_api_top_items('tracks', token)
    tracks_df = pd.DataFrame(top_tracks)
    tracks_df.to_csv(df_path_tracks)

    print('Retrieving top artists for myself.')
    top_artists = get_api_top_items('artists', token)
    artists_df = pd.DataFrame(top_artists)
    artists_df.to_csv(df_path_artists)


if __name__ == "__main__":
    get_top_items()