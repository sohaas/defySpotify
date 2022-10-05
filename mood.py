"""
Majority of this was shamelessly stolen from https://github.com/vlad-ds/spoty-records
- thanks go out to my man Vlad
"""
import json
import pandas as pd
from os import listdir
from collections import namedtuple

from utils.utils import get_features, check_file_exists
from utils.api_token import get_api_token
from utils.api_requests import get_api_features, get_api_playlists, get_api_playlist_items
from utils.parse_data import read_user_id, read_endsong, read_features, read_playlist_mood
from utils.plots import plot_endsong_mood, plot_playlist_mood


def get_audio_features(subject="001"):

    token = get_api_token(scope='user-read-recently-played')
    
    path = 'data/' + subject + '/MyData 2/'
    csv_path = 'output/helpers/' + subject + '/features.csv'

    files = [path + x for x in listdir(path)
             if x.split('.')[0][:-1] == 'endsong_']
    streamings = read_endsong(path, len(files))
    streamings = streamings[['timestamp', 'ms_played', 'track_name', 'artist_name', 'album_name', 'track_uri']]
    streamings['track_id'] = streamings['track_uri'].str.split('spotify:track:', 1, expand=True)[1]

    tracks = streamings.drop_duplicates(subset = 'track_uri')
    print(f'Discovered {len(streamings)} tracks in total, of which {len(tracks)} are unique.')

    print('Connecting to Spotify to extract features...')
    acquired = 0
    track_features = {track: None for track in tracks['track_id']}
    for track in tracks['track_id']:
        try:
            features = get_api_features(track, token)
            track_features[track] = features
            if features:
                acquired += 1
        except:
            features = None
        if acquired % 250 == 0 and acquired != 0:
            print(f'Already recovered features of {acquired} tracks.')
        # handle access token expire
        if acquired % 5000 == 0:
            token = get_api_token(scope='user-read-recently-played')

    print(f'Successfully recovered features of {acquired} tracks in total.')
    if len(tracks) - acquired != 0:
        print(f'Failed to identify {len(tracks) - acquired} items. Some of these may not be tracks.')
    
    features_df = pd.DataFrame(track_features).T
    features_df.to_csv(csv_path)
    print(f'Saved features to {csv_path}.')

    print('Adding features to streamings...')
    streamings_with_features = []
    streamings = streamings.sort_values(by=['timestamp'])
    for index, streaming in streamings.iterrows():
        track = streaming['track_id']
        features = features_df.loc[track]
        if features is not None:
            streamings_with_features.append({**streaming, **features})

    print(f'Added features to {len(streamings_with_features)} streamings.')
    print('Saving streamings...')
    df_final = pd.DataFrame(streamings_with_features)
    df_final.to_csv(csv_path)
    perc_featured = round(len(streamings_with_features) / len(streamings) * 100, 2)
    print(f"Done! Percentage of streamings with features: {perc_featured}%.")


def get_history_mood(subject="001"):
    
    path = 'output/helpers/' + subject + '/features.csv'
    plot_path = 'output/' + subject + '/history_mood.png'

    print('Retrieving collection mood for subject ' + subject)

    if not check_file_exists(path, False):
        get_audio_features(subject)
    df = read_features(path)

    df['sec_played'] = df['ms_played'] / 1000
    df = df[df.columns[:-1].insert(4, df.columns[-1])]
    df = df[df.sec_played > 60]

    df['month'] = df.timestamp.str.split('-').apply(lambda x: (x[0], x[1]))
    months = list(set(df.month.values))
    months.sort()

    features = get_features()
    
    for feature in features:
        df[f'{feature}_zscore'] = ( df[feature] - df[feature].mean() ) / df[feature].std()
    df[[feature + '_zscore' for feature in features]].describe().loc['mean':'std'].T

    Month = namedtuple('Month', features)
    avg_features_months = []
    for month in months:
        df_month = df[df['month'] == month]
        avg_features = df_month.describe().loc['mean'][[feature + '_zscore' for feature in features]]
        month = Month(*avg_features)
        avg_features_months.append(month)

    plot_endsong_mood(months, avg_features_months, plot_path)


def get_playlists(subject="001"):

    path = 'data/' + subject + '/'
    csv_path = 'output/helpers/' + subject + '/features.csv'
    df_path = 'output/' + subject + '/playlists.csv'

    if check_file_exists(df_path, False):
        print('Playlist file already exists for subject ' + subject)
        return
    else:
        print('Retrieving playlists for subject ' + subject)

    token = get_api_token('playlist-read-private')
    user_id = read_user_id(path)
    playlists = get_api_playlists(user_id, token)

    if not playlists:
        print('Subject ' + subject + ' has no own Spotify playlists.')
        return

    playlists_df = pd.DataFrame.from_records(playlists)
    # remove owner id if it's the user's
    playlists_df.to_csv(df_path)
    playlists_df.loc[playlists_df['owner_id'] == user_id, 'owner_id'] = subject

    # replace owner id in identity file with subject number
    id_path = 'data/' + subject + '/Identity.json'
    file = open(id_path)
    data = json.load(file)
    data['username'] = subject
    with open(id_path, 'w') as outfile:
        json.dump(data, outfile)

    features = get_features()

    history_features_df = read_features(csv_path)
    avg_features_df = pd.DataFrame()

    i = 0
    for playlist in playlists:
        
        print('Getting features for playlist number ' + str(i))
        i = i + 1

        # get track ids of each playlist
        track_ids = get_api_playlist_items(playlist['id'], token)
        if not track_ids:
            continue

        # get audio features of each playlist
        playlist_features = []
        for track_id in track_ids:
            track_features = get_api_features(track_id, token)
            if track_features:
                playlist_features.append(track_features)
        features_df = pd.DataFrame(playlist_features)

        # calculate mean from audio features
        for feature in features:
            features_df[f'{feature}_zscore'] = ( features_df[feature] - history_features_df[feature].mean() ) / history_features_df[feature].std()
        features_df[[feature + '_zscore' for feature in features]].describe().loc['mean':'std'].T
        avg_features = features_df.describe().loc['mean'][[feature + '_zscore' for feature in features]]
        
        avg_features_df = avg_features_df.append(avg_features)

    avg_features_df = avg_features_df.reset_index(drop=True)
    joined_df = pd.concat([playlists_df, avg_features_df], axis=1)
    joined_df.to_csv(df_path)


def get_playlist_mood(subject="001"):

    path = 'output/' + subject + '/playlists.csv'
    plot_path = 'output/' + subject + '/playlist_mood.png'

    print('Retrieving playlist mood for subject ' + subject)
    
    if not check_file_exists(path, False):
        get_playlists(subject)
    names, avg_features_playlist = read_playlist_mood(path)

    if avg_features_playlist is not None:
        plot_playlist_mood(names, avg_features_playlist, plot_path)


if __name__ == "__main__":
    get_history_mood("001")
    get_playlist_mood("001")