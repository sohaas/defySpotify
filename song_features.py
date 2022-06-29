import ast
import requests
import spotipy
import pandas as pd
from os import listdir
from time import sleep
from datetime import datetime


def get_streamings(path):
    
    files = [path + x for x in listdir(path)
             if x.split('.')[0][:-1] == 'StreamingHistory']
    
    all_streamings = []
    
    for file in files: 
        with open(file, 'r', encoding='UTF-8') as f:
            new_streamings = ast.literal_eval(f.read())
            all_streamings += [streaming for streaming 
                               in new_streamings]
    
    for streaming in all_streamings:
        streaming['datetime'] = datetime.strptime(streaming['endTime'], '%Y-%m-%d %H:%M')   
         
    return all_streamings


def get_api_id(track_info, token):

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ' + token,
    }
    track_name = track_info.split("___")[0]
    params = [
        ('q', track_name),
        ('type', 'track'),
    ]
    artist = track_info.split("___")[-1]
    if artist:
        params.append(('artist', artist))

    try:
        response = requests.get('https://api.spotify.com/v1/search', 
                    headers = headers, params = params, timeout = 5)
        json = response.json()
        results = json['tracks']['items']
        first_result = json['tracks']['items'][0]
        if artist:
            for result in results:
                if artist.strip() == result['artists'][0]['name'].strip():
                    track_id = result['id']
                    return track_id
        track_id = first_result['id']
        return track_id
    except:
        return None


def get_saved_ids(tracks, path):

    track_ids = {track: None for track in tracks}
    folder, filename = path.split('/')
    if filename in listdir(folder):
        try:
            idd_dataframe = pd.read_csv('output/track_ids.csv', 
                                     names = ['name', 'idd'])
            idd_dataframe = idd_dataframe[1:]
            added_tracks = 0
            for index, row in idd_dataframe.iterrows():
                if not row[1] == 'nan':
                    track_ids[row[0]] = row[1]
                    added_tracks += 1
            print(f'Saved IDs successfully recovered for {added_tracks} tracks.')
        except:
            print('Error. Failed to recover saved IDs!')
            pass
    return track_ids


def get_api_features(track_id, token):

    sp = spotipy.Spotify(auth=token)
    try:
        features = sp.audio_features([track_id])
        return features[0]
    except:
        return None


def get_album(track_id: str, token: str) -> dict:

    sp = spotipy.Spotify(auth=token)
    try:
        album = sp.track(track_id)
        album_id = album['album']['id']
        album_name = album['album']['name']
        return album_name, album_id
    except:
        return None, None


def get_saved_features(tracks, path):

    folder, file = path.split('/')
    track_features = {track: None for track in tracks}
    if file in listdir(folder):
        features_df = pd.read_csv(path, index_col = 0)
        n_recovered_tracks = 0
        for track in features_df.index:
            features = features_df.loc[track, :]
            if not features.isna().sum():
                track_features[track] = dict(features)
                n_recovered_tracks += 1
        print(f"Added features for {n_recovered_tracks} tracks.")
        return track_features
    else:
        print("Did not find features file.")
        return track_features


def build_df():

    username = 'hc112g6npbzt6gogaywqhah2l'
    client_id ='f0b2325db15a42cb8a8abc634aed582b'
    client_secret = '3f06cd1b404c42dbaa207aa506515f7d'
    redirect_uri = 'http://localhost:7777/callback'
    scope = 'user-read-recently-played'

    token = spotipy.util.prompt_for_user_token(username=username, 
                                    scope=scope, 
                                    client_id=client_id,   
                                    client_secret=client_secret,     
                                    redirect_uri=redirect_uri)

    path = '/Users/sophiehaas/Downloads/MyData/'
    streamings = get_streamings(path)
    tracks = set([f"{streaming['trackName']}___{streaming['artistName']}" for streaming in streamings])
    print(f'Discovered {len(tracks)} unique tracks.')

    track_ids = get_saved_ids(tracks, 'output/track_ids.csv')
    tracks_missing_idd = len([track for track in tracks if track_ids.get(track) is None])

    if tracks_missing_idd > 0:
        sleep(3)
        id_length = 22
        for track, idd in track_ids.items(): 
            if idd is None:
                try:
                    found_idd = get_api_id(track, token)
                    track_ids[track] = found_idd
                except:
                    pass
    
        identified_tracks = [track for track in track_ids
                            if track_ids[track] is not None]
        print(f'Successfully recovered the ID of {len(identified_tracks)} tracks.')
        n_tracks_without_id = len(track_ids) - len(identified_tracks)
        print(f"Failed to identify {n_tracks_without_id} items. "
                "However, some of these may not be tracks (e.g. podcasts).")
        
        ids_path = 'output/track_ids.csv'
        ids_dataframe = pd.DataFrame.from_dict(track_ids, 
                                                orient = 'index')
        ids_dataframe.to_csv(ids_path)
        print(f'Track ids saved to {ids_path}.')

    track_features = get_saved_features(tracks, 'output/features.csv')
    tracks_without_features = [track for track in tracks if track_features.get(track) is None]
    path = 'output/features.csv'

    if len (tracks_without_features):
        print('Connecting to Spotify to extract features...')
        acquired = 0
        for track, idd in track_ids.items():
            if idd is not None and track in tracks_without_features:
                try:
                    features = get_api_features(idd, token)
                    track_features[track] = features
                    features['albumName'], features['albumID'] = get_album(idd, token)
                    if features:
                        acquired += 1
                except:
                    features = None
        tracks_without_features = [track for track in tracks if track_features.get(track) is None]
        print(f'Successfully recovered features of {acquired} tracks.')
        if len(tracks_without_features):
            print(f'Failed to identify {len(tracks_without_features)} items. Some of these may not be tracks.')
    
        features_dataframe = pd.DataFrame(track_features).T
        features_dataframe.to_csv(path)
        print(f'Saved features to {path}.')
        
    print('Adding features to streamings...')
    streamings_with_features = []
    for streaming in sorted(streamings, key= lambda x: x['endTime']):
        track = streaming['trackName'] + "___" + streaming['artistName']
        features = track_features.get(track)
        if features:
            streamings_with_features.append({'name': track, **streaming, **features})
    print(f'Added features to {len(streamings_with_features)} streamings.')
    print('Saving streamings...')
    df_final = pd.DataFrame(streamings_with_features)
    df_final.to_csv('output/mood.csv')
    perc_featured = round(len(streamings_with_features) / len(streamings) *100, 2)
    print(f"Done! Percentage of streamings with features: {perc_featured}%.") 