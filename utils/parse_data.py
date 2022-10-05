import re
import json
import pytz
import pandas as pd
from datetime import datetime
from collections import namedtuple, deque

from utils.utils import unix_to_date, str_to_datetime, get_features, check_file_exists

utc = pytz.UTC


################################ READ RAW DATA ##################################

def read_endsong(root, file_count):
    df_endsong = pd.DataFrame()
    for i in range(file_count):
        file_path = f'{root}/endsong_{i}.json'
        df = pd.read_json(file_path)
        # df['source_file'] = i
        df_endsong = pd.concat(
            [df_endsong, df], axis=0, ignore_index=True)

    df_endsong.rename(columns={
        'ip_addr_decrypted': 'ip_addr',
        'master_metadata_track_name': 'track_name',
        'master_metadata_album_artist_name': 'artist_name',
        'master_metadata_album_album_name': 'album_name',
        'spotify_track_uri': 'track_uri',
        'spotify_episode_uri': 'episode_uri',
        'incognito_mode': 'private_session',
        'ts': 'timestamp'
    }, inplace=True)

    df_endsong['timestamp'] = df_endsong['timestamp'].apply(lambda x: datetime.strptime(
            x, '%Y-%m-%dT%H:%M:%SZ'))
    df_endsong['timestamp'] = pd.to_datetime(df_endsong.timestamp, utc=True)

    return df_endsong


def read_SportyFormatlistRequest(subject_nr):
    df = pd.read_json(f'./raw_data/{subject_nr}/SportyFormatlistRequest.json')
    df = df.sort_values(by='context_time')
    df.rename(columns={
        'message_playlist_id': 'playlist_id',
        'message_playlist_version': 'playlist_version',
        'context_time': 'timestamp'
    }, inplace=True)
    df = df[["timestamp", "playlist_id", "playlist_version"]]

    return df


def read_BasslineRequests(subject_nr):
    """
    Reads and preprocesses BasslineRequests.json

    File Description:
    This event contains information about requests made from Spotify clients to fetch Spotify 
    entity metadata (such as for instance tracks, albums and playlists) from Spotify backend 
    services. Only requests for logged in users will generate these events. These events are 
    the basis for aggregated stats about usage of our systems.

    Original data fields:
        timestamp_utc:          Not documented
        context_time:           Unix timestamp
        message_country:        User's account country
        message_product:        Current Spotify version ('premium', 'free')
        message_query_hash:     Server instruction (how to structure the data)
        message_operation_name: Server instruction (which data to return)
        message_variables:      Server instruction (which data to return, e.g., which artist)
        message_errors:         Success status of request
        message_user_agent:     Current client user agent
        message_region:         Region where the request was received (e.g., Europe, Asia)
        message_ms_latency:     Processing time

    Returned data fields:
        timestamp_utc:          timestamp in UTC format
        timestamp_unix:         timestamp in Unix format
        content:                Requested data
    """
    df = pd.read_json(f'./raw_data/{subject_nr}/BasslineRequests.json')
    df = df.sort_values(by='context_time')
    df.rename(columns={
        'context_time': 'timestamp_unix',
        'message_variables': 'content'
    }, inplace=True)
    df = df[["timestamp_utc", "timestamp_unix", "content"]]
    return df


def read_KmInteraction(subject_nr):
    """
    Reads and preprocesses KmInteraction.json

    File description:
    This event is emitted when a user interacts with the UI on the Web Player or Desktop Client.

    Original data fields:
        timestamp_utc:                      Not documented
        context_time:                       Unix timestamp
        context_receiver_service_timestamp: Time at which the Spotify server received the event
        context_user_agent:                 OS, OS version, browser, browser version
        context_application_version:        Application version
        context_conn_country:               Origin country of the request (in ISO-3166-1 alpha-2 code)
        message_page:                       Identifier of the page the user interacted with
        message_view_uri:                   Spotify URI of entity where interaction took place
        message_target_uri:                 URI of interaction target element (e.g., track URI if 
                                            track was clicked)
        message_item_id:                    visual location of the element in the UI
        message_action_type:                action type of the interaction, e.g., 'click'
        message_action_intent:              action intent, e.g., 'navigate

    Returned data fields:
        timestamp_utc:                      timestamp in UTC format
        timestamp_unix:                     timestamp in Unix format
        page:                               Identifier of the page the user interacted with
        view_uri:                           Spotify URI of entity where interaction took place
        target_uri:                         URI of interaction target element (e.g., track URI if 
                                            track was clicked)
        item_location:                      visual location of the element in the UI
        action_type:                        action type of the interaction, e.g., 'click'
        action_intent:                      action intent, e.g., 'navigate
    """
    df = pd.read_json(f'./raw_data/{subject_nr}/KmInteraction.json')
    df = df.sort_values(by='context_time')
    df.rename(columns={
        'context_time': 'timestamp_unix',
        'message_page': 'page',
        'message_view_uri': 'view_uri',
        'message_target_uri': 'target_uri',
        'message_item_id': 'item_location',
        'message_action_type': 'action_type',
        'message_action_intent': 'action_intent'
    }, inplace=True)
    df = df[[
        "timestamp_utc", "timestamp_unix", "page", "view_uri", "target_uri", "item_location",
        "action_type", "action_intent"]]
    return df


def read_ParadoxCampaignOptimizerEvent(subject_nr):
    """
    Reads and preprocesses ParadoxCampaignOptimizerEvent.json

    File description:
    This event record log messages from Spotify's internal message optimization service.

    Original data fields:
        timestamp_utc:                  Not documented
        context_time:                   Unix timestamp
        message_campaign_id:            Campaign ID
        message_content_uri:            URI of the campaign content
        message_business_objective:     Business objective, e.g., ENGAGEMENT, CONTENT_PROMOTION
        message_priority:               Priority of the shown message
        message_channel:                Channel used to show the message, e.g., INAPP
        message_transactional:          Transactional flag used
        message_reorder_occured:        Indication of whether the message list was reordered
        message_rank:                   Rank in the resulting list of messages shown
        message_targeting_parameters:   Number of messages within the targeting parameters
        message_score:                  Score used to rank the resulting list
        message_sort_method:            Name of used sorting method
        message_model_name:             Name of used model
        message_model_revision:         Revision number of used model
        message_message_id:             Message ID of the campaign
        message_locale:                 User's locale for this request, e.g., de_DE
        message_region:                 Region that served the request, e.g., europe-west1
        message_score_threshold:        Threshold applied to request for filtering responses based
                                        on model score
        message_filter_flag:            Indication of whether the message was filtered from the 
                                        response
        message_action_url:             URL destination of Primary Action Button
        message_action_url_type:        Type classification of Primary Action Button, e.g., MUSIC,
                                        EXTERNAL
        message_salem_score:            Raw score from salem problem endpoints

    Returned data fields:
        timestamp_utc:                  timestamp in UTC format
        timestamp_unix:                 timestamp in Unix format
        business_objective:             Business objective, e.g., ENGAGEMENT, CONTENT_PROMOTION
        action_url:                     URL destination of Primary Action Button
        action_url_type:                Type classification of Primary Action Button, e.g., MUSIC,
                                        EXTERNAL 
    """
    df = pd.read_json(f'./raw_data/{subject_nr}/ParadoxCampaignOptimizerEvent.json')
    df = df.sort_values(by='context_time')
    df.rename(columns={
        'context_time': 'timestamp_unix',
        'message_business_objective': 'business_objective',
        'message_channel': 'channel',
        'message_action_url': 'action_url',
        'message_action_url_type': 'action_url_type',
    }, inplace=True)
    df = df[[
        "timestamp_utc", "timestamp_unix", "business_objective", "channel",
        "action_url", "action_url_type"]]
    return df


def read_PartnerNaturalLanguageAction(subject_nr):
    """
    Reads and preprocesses PartnerNaturalLanguageAction.json

    File description:
    This event tracks voice integration requests for playback. This call is usually made 
    from the speaker to initiate playback.

    Original data fields:
        timestamp_utc:          Not documented
        context_time:           Unix timestamp
        message_catalogue:      Current Spotify version ('premium', 'free')
        message_country:        Country of the account (in ISO-3166-1 alpha-2 code)
        message_context_url:    URL of the selected content
        message_device_id:      ID of the device the play will be initiated on
        message_device_brand:   Brand of the device, e.g., 'Google', 'Apple'
        message_device_model:   Model of the device, e.g., 'Iphone', 'Google Home'
        message_voice_enabled:  Whether the device has a microphone and voice is enabled

    Returned data fields:
        timestamp_utc:          timestamp in UTC format
        timestamp_unix:         timestamp in Unix format
        context_url:            URL of the selected content
    """
    df = pd.read_json(f'./raw_data/{subject_nr}/PartnerNaturalLanguageAction.json')
    df = df.sort_values(by='context_time')
    df.rename(columns={
        'context_time': 'timestamp_unix',
        'message_context_url': 'context_url'
    }, inplace=True)
    df = df[[
        "timestamp_utc", "timestamp_unix", "context_url"]]
    return df


def read_PartnerNaturalLanguageIntentResolution(subject_nr):
    """
    Reads and preprocesses PartnerNaturalLanguageIntentResolution.json

    File description:
    This event contains the information that is generated following an utterance made by a 
    user to a voice assistant, including the resolved intent as well as the Uniform Resource 
    Identifier (URI) of the audio content being played. For instance, for the utterance "Play 
    madonna on spotify", the resolved intent will be "play" and the URI will be 
    "spotify:artist:xxx"

    Original data fields:
        timestamp_utc:              Not documented
        context_time:               Unix timestamp
        message_catalogue:          Current Spotify version ('premium', 'free')
        message_country:            Country of the account (in ISO-3166-1 alpha-2 code)
        message_device_id:          ID of the device the play will be initiated on
        message_device_brand:       Brand of the device, e.g., 'Google', 'Apple'
        message_device_model:       Model of the device, e.g., 'Iphone', 'Google Home'
        message_voice_enabled:      Whether the device has a microphone and voice is enabled
        message_original_uri:       URI suggested by the voice assistant's natural language
                                    understanding service
        message_text_query:         Text version of the utterance made by the user via voice
        message_language:           Language of the user's utterance by BCP-47 language tag
        message_final_uri:          URI of the content that will be played
        message_voice_assistant:    Name of the voice assistant on the device, e.g. Amazon 
                                    Alexa

    Returned data fields:
        timestamp_utc:              timestamp in UTC format
        timestamp_unix:             timestamp in Unix format
        original_uri:               URI suggested by the voice assistant's natural language
                                    understanding service
        utterance:                  Text version of the utterance made by the user via voice
        final_uri:                  URI of the content that will be played
    """
    df = pd.read_json(f'./raw_data/{subject_nr}/PartnerNaturalLanguageIntentResolution.json')
    df = df.sort_values(by='context_time')
    df.rename(columns={
        'context_time': 'timestamp_unix',
        'message_original_uri': 'original_uri',
        'message_text_query': 'utterance',
        'message_final_uri': 'final_uri'
    }, inplace=True)
    df = df[[
        "timestamp_utc", "timestamp_unix", "original_uri", "utterance", "final_uri"]]
    return df


def read_PlaybackFromDeeplink(subject_nr):
    """
    Reads and preprocesses PlaybackFromDeeplink.json

    File description:
    This event is emitted when playback is initiated on the Spotify application following a 
    deeplink redirect.

    Original data fields:
        timestamp_utc:                      Not documented
        context_time:                       Unix timestamp
        context_receicer_service_timestamp: Unix timestamp of when server received event
        context_user_agent:                 OS, OS version, browser, browser version
        context_application_version:        Application version
        context_conn_country:               Country the connection originated from 
        context_device_manufacturer:        Device manufacturer
        context_device_model:               Device model
        context_os_name:                    OS
        context_os_version:                 OS version
        message_playing_entity_uri:         URI of the playing entity
        message_playing_context_uri:        URI of the playing entity context

    Returned data fields:
        timestamp_utc:                      timestamp in UTC format
        timestamp_unix:                     timestamp in Unix format
        entity_uri:                         URI of the playing entity
        context_uri:                        URI of the playing entity context
    """
    df = pd.read_json(f'./raw_data/{subject_nr}/PlaybackFromDeeplink.json')
    df = df.sort_values(by='context_time')
    df.rename(columns={
        'context_time': 'timestamp_unix',
        'message_playing_entity_uri': 'entity_uri',
        'message_playing_context_uri': 'context_uri',
    }, inplace=True)
    df = df[[
        "timestamp_utc", "timestamp_unix", "entity_uri", "context_uri"]]
    return df


def read_ReleaseRadarServedRecs(subject_nr):
    """
    Reads and preprocesses ReleaseRadarServedRecs.json

    File description:
    The sequence of tracks served to a user when they listened to a Release Radar playlist.

    Original data fields:
        timestamp_utc:                  Not documented
        context_time:                   Unix timestamp
        message_registration_country:   Registration country of the user
        message_context_uri:            URI of the content set
        message_is_made_for_user:       Whether user is viewing their own playlist
        message_track_uris:             Ordered list of tracks served
    """
    df = pd.read_json(f'./raw_data/{subject_nr}/ReleaseRadarServedRecs.json')
    df = df.sort_values(by='context_time')
    df.rename(columns={
        'context_time': 'timestamp_unix',
        'message_is_made_for_user': 'made_for_user',
        'message_context_uri': 'context_uri',
        'message_track_uris': 'track_uris'
    }, inplace=True)
    df = df[[
        "timestamp_utc", "timestamp_unix", "made_for_user", "context_uri", "track_uris"]]
    return df


def read_VoiceContentCreated(subject_nr):
    """
    Reads and preprocesses VoiceContentCreated.json

    File description:
    This event is emitted when a voice request is processed for an intended playback.

    Original data fields:
        timestamp_utc:                      Not documented
        context_time:                       Unix timestamp
        message_timestamp:                  Timestamp of when the event was emitted
        message_voice_assistant:            Voice assistant type
        message_catalogue:                  Current Spotify version ('premium', 'free')
        message_country:                    Country of the user account
        message_response_uri:               Spotify content URI based on the request
        message_filter_explicit_content:    Whether the request is filtering explicit content
        message_on_demand_trial:            Whether user account is on-demand-trial
        message_device_type:                Type of playback device
        message_voice_enabled:              Whether the requesting device has a microphone
        message_shuffle:                    Whether response content has shuffle turned on

    """
    df = pd.read_json(f'./raw_data/{subject_nr}/VoiceContentCreated.json')
    df = df.sort_values(by='context_time')
    df.rename(columns={
        'context_time': 'timestamp_unix',
        'message_timestamp': 'timestamp_iso',
        'message_response_uri': 'content_uri',
        'message_filter_explicit_content': 'filtered',
        'message_shuffle': 'shuffle'
    }, inplace=True)
    df = df[[
        "timestamp_utc", "timestamp_unix", "timestamp_iso", "content_uri", "filtered", "shuffle"]]
    return df


def read_inferences(path):
    if not check_file_exists(path):
        return None

    # sorted interest-wise
    categories = ['News', 'History', 'Books', 'Studying or focusing', 'Education', 'Science & Medicine', 'Tech', 'Business', 'Commuting', 'In-car listening', 'Travel', 'Culture & Society', 'Love &\nDating', 'Parenting', 'Cooking', 'Health & Lifestyle', 'Fitness', 'Running', 'Sports & Recreation', 'DIY Hobbies & Crafts', 'Partying', 'Comedy', 'Gaming', 'Theater', 'TV & Film', 'Podcasts']

    file = open(path + 'Inferences.json')
    data = json.load(file)
    inferences = data['inferences']

    values = []
    for category in categories:
        if category in inferences:
            values.append(100)
        else:
            values.append(0)

    values += values[:1]
    return values


def read_user_id(path):
    if not check_file_exists(path):
        return None

    file = open(path + 'Identity.json')
    user_data = json.load(file)
    user_id = user_data['username']

    return user_id


############################ READ PROCESSED DATA ################################

def read_playlist_df(filepath):
    if not check_file_exists(filepath):
        return None

    # TODO: fix format
    playlist_df = pd.read_csv(filepath)
    playlist_df['timestamp'] = playlist_df['timestamp'].apply(str_to_datetime)
    return playlist_df
   

def read_listening_behavior(path, subject_nr):
    df = pd.read_csv(f'./processed_data/{subjecz_nr}/listening_behavior_subject1.csv')
    df = df[df['timestamp'].notna()]
    df['timestamp'] = df['timestamp'].apply(str_to_datetime)
    return df


def read_features(path):
    if not check_file_exists(path):
        return None

    return pd.read_csv(path, index_col = 0)


def read_playlist_mood(path):
    if not check_file_exists(path):
        return None, None

    df = pd.read_csv(path, index_col = 0)
    features = get_features()

    playlists = list(set(df.name.values))
    playlists.sort()

    Playlist = namedtuple('Playlist', features)
    names = []
    avg_features_playlist = []
    for playlist in playlists:
        df_playlist = df[df['name'] == playlist]
        #names.append(re.sub(r"[^A-Za-z .,\-!?]+", '', playlist))
        names.append(playlist)
        avg_features = df_playlist.describe().loc['mean'][[feature + '_zscore' for feature in features]]
        playlist = Playlist(*avg_features)
        avg_features_playlist.append(playlist)

    return names, avg_features_playlist


def read_UI_playlists(subject_nr):
    """
    Reads the data of UI based playlist interactions from csv file.

    Args:
        subject_nr (str): The number of the subject whose data should be read.

    Returns:
        pandas dataframe of the read data
    """
    return pd.read_csv(
        f'./processed_data/{subject_nr}/kmInteraction_playlists_{subject_nr}.csv',
        index_col=0)


def read_voice_playlists(subject_nr):
    """
    Reads the data of voice based playlist interactions from csv file.

    Args:
        subject_nr (str): The number of the subject whose data should be read.

    Returns:
        pandas dataframe of the read data
    """
    return pd.read_csv(
        f'./processed_data/{subject_nr}/PartnerNaturalLanguageIntentResolution_playlists_{subject_nr}.csv',
        index_col=0)


def read_locations(subject_nr):
    """
    Reads the location data from csv file.

    Args:
        subject_nr (str): The number of the subject whose data should be read.

    Returns:
        pandas dataframe of the read data
    """
    return pd.read_csv(
        f'./processed_data/{subject_nr}/locations.csv',
        index_col=0)