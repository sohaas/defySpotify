import requests
import spotipy
from urllib.request import urlopen
from json import load


from utils.api_token import get_api_token


def request_playlist_info(playlist_id, token):
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ' + token,
    }
    try:
        response = requests.get('https://api.spotify.com/v1/playlists/' + playlist_id, headers = headers, timeout = 5)
        return response.json()
    except:
        return None


def get_api_artists(track_id, token):

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ' + token,
    }

    try:
        response = requests.get('https://api.spotify.com/v1/tracks/' + track_id, headers = headers, timeout = 5)
        json = response.json()
        return json['artists'][0]['id']
    except:
        return None


def get_api_genres(artist_id, token):

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ' + token,
    }

    try:
        response = requests.get('https://api.spotify.com/v1/artists/' + artist_id, headers = headers, timeout = 5)
        json = response.json()
        return json['genres']
    except:
        return None


def get_api_playlists(user_id, token):

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ' + token,
    }

    try:
        response = requests.get('https://api.spotify.com/v1/users/' + user_id + '/playlists', headers = headers, timeout = 5)
        json = response.json()
        playlists = []
        for item in json['items']:
            #TODO this is fucking ugly
            playlist = {'name': item['name'],
                        'id': item['id'], 
                        'description': item['description'], 
                        'owner_id': item['owner']['id'],
                        'collaborative': item['collaborative'], 
                        'public': item['public'], 
                        'tracks_href': item['tracks']['href'],
                        'tracks_total': item['tracks']['total'],}
            playlists.append(playlist)
        return playlists
    except:
        return None


def get_api_playlist_items(playlist_id, token):

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ' + token,
    }

    try:
        response = requests.get('https://api.spotify.com/v1/playlists/' + playlist_id + '/tracks', headers = headers, timeout = 5)
        json = response.json()
        track_ids = []
        for items in json['items']:
            track_id = items['track']['id']
            track_ids.append(track_id)
        return track_ids
    except:
        return None


def get_api_podcasts(show_id, token):

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ' + token,
    }

    try:
        response = requests.get('https://api.spotify.com/v1/episodes/' + show_id, headers = headers, timeout = 5)
        json = response.json()
        """
        other categories that might also be interesting:
            explicit, is_externally_hosted, resume_point: fully_played, 
            resume_point: resume_position_ms
        """
        #TODO this is fucking ugly
        episode = {'description': json['description'],
                   'duration_ms': json['duration_ms'], 
                   'language': json['language'], 
                   'languages': json['languages'], 
                   'release_date': json['release_date'], 
                   'show_description': json['show']['description'],
                   'show_publisher': json['show']['publisher']}
        return episode
    except:
        return None


def get_api_top_items(type, token):

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ' + token,
    }

    try:
        response = requests.get('https://api.spotify.com/v1/me/top/' + type, headers = headers, timeout = 5)
        json = response.json()
        top_items = []
        for item in json['items']:
            if type == 'tracks':
                item = {'name': item['name'],
                        'artist': item['artists'][0]['name'],
                        'popularity': item['popularity'],
                        'id': item['id']}
            elif type == 'artists':
                item = {'name': item['name'],
                        'genres': item['genres'],
                        'followers': item['followers']['total'],
                        'popularity': item['popularity'],
                        'id': item['id']}
            else:
                item = None
            top_items.append(item)
        return top_items
    except:
        return None


def get_api_features(track_id, token):

    sp = spotipy.Spotify(auth=token)
    try:
        features = sp.audio_features([track_id])
        return features[0]
    except:
        return None


def get_ip_info(ip_address):
    if ip_address == '':
        url = 'https://ipinfo.io/json'
    else:
        url = 'https://ipinfo.io/' + ip_address + '/json'

    res = urlopen(url)
    return load(res)
