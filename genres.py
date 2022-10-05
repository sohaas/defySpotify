"""
TODO For the funsies: Get available genre seeds:
     https://developer.spotify.com/documentation/web-api/reference/#/operations/get-recommendation-genres
"""

import pandas as pd

from utils.utils import check_file_exists
from utils.api_token import get_api_token
from utils.api_requests import get_api_artists, get_api_genres
from utils.parse_data import read_features
from utils.plots import plot_genres


def get_genres(subject="001"):

    token = get_api_token(scope='user-read-recently-played')
    path = 'output/helpers/' + subject + '/genres.csv'
    csv_path = 'output/helpers/' + subject + '/features.csv'

    print('Retrieving genres for subject ' + subject)

    if check_file_exists(path, False):
        print('Genres file already exists for subject ' + subject)
        return

    features_df = read_features(csv_path)
    artists = features_df.drop_duplicates(subset = 'artist_name')
    print(f'Discovered {len(artists)} unique artists.')

    print('Connecting to Spotify to extract genres...')
    artist_genres = []
    acquired = 0
    for index, artist in artists.iterrows():
        track_id = artist['track_id']
        try:
            # request artist id with track id
            artist_id = get_api_artists(track_id, token)
            # request genre with artist id
            genre = get_api_genres(artist_id, token)
            if genre: 
                artist_genres.append({'name': artist['artist_name'], 'genre': genre})
                acquired += 1
            else:
                artist_genres.append({'name': artist['artist_name'], 'genre': None})
        except:
            artist_genres.append({'name': artist['artist_name'], 'genre': None})

        if acquired % 250 == 0 and acquired != 0:
            print(f'Already recovered genres of {acquired} artists.')
        if acquired % 5000 == 0:
            token = get_api_token(scope='user-read-recently-played')

    print(f'Successfully recovered genres of {acquired} artists in total.')
    if len(artists) - acquired != 0:
        print(f'Failed to identify {len(artists) - acquired} items.')
    genres_df = pd.DataFrame.from_records(artist_genres)

    # count occuences of artists in all streamings
    genre_count = []
    for index, row in genres_df.iterrows():
        artist = row['name']
        count = len(features_df[features_df['artist_name'] == artist])
        genre_count.append({'genre': row['genre'], 'count': count})

    # get unique genres
    count_df = pd.DataFrame.from_records(genre_count)
    count_df = count_df.explode('genre')
    count_df = count_df.groupby(by=['genre']).sum()
    count_df = count_df.sort_values('count', ascending=False)
    count_df = count_df.reset_index(level=0)
    count_df.to_csv(path)
    print('Discovered ' + str(len(count_df)) + ' unique genres.')

    # plot data
    plot_genres(subject, count_df)


if __name__ == "__main__":
    get_genres("001")
