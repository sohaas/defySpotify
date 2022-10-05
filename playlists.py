import json
import pandas as pd

from utils.utils import create_sessions, unix_to_date
from utils.api_token import get_api_token
from utils.plots import *
from utils.parse_data import *
from utils.api_requests import request_playlist_info


############################## PROCESS DATA FILES ############################

def process_SportyFormatlistRequest():
    """
    Processes data from SportyFormatlistRequest.json for playlist analysis. 
    See read_SportyFormatListRequest() for more information on the contained data.
    """
    pass

def process_BasslineRequests():
    """
    Processes data from BasslineRequests.json for playlist analysis. 
    See read_BasslineRequests() for more information on the contained data.
    """
    pass

def process_KmInteraction(subject_nr ,request=False):
    """
    Processes data from KmInteraction.json for playlist analysis. 
    See read_KmInteraction() for more information on the contained data.

    Args:
        subjectNr(str):     Number assigned to the subject
        request(boolean):   True when playlist info needs to be requested, False otherwise
    """
    df = read_KmInteraction(subject_nr)

    # check values (print one of the variables to see which values the data field contains)
    pages = list(set(df.page.tolist()))
    view_uris = list(set(df.view_uri.tolist()))
    target_uris = list(set(df.target_uri.tolist()))
    item_locations = list(set(df.item_location.tolist()))
    action_types = list(set(df.action_type.tolist()))
    action_intents = list(set(df.action_intent.tolist()))
    
    # collect all entries that involve a playlist
    df = df.loc[(df['page']=='playlist') | (df['view_uri'].str.contains('playlist')) | \
        (df['target_uri'].str.contains('playlist')) | (df['item_location'].str.contains('playlist'))]

    # filter entries with action intent play, go-to-radio and add-to-queue
    df = df[df['action_intent'].isin(['play', 'go-to-radio', 'add-to-queue'])]

    # extract unique playlist ids
    playlist_view_uris = df[df['view_uri'].str.contains('playlist')]['view_uri']
    playlist_view_ids = playlist_view_uris.apply(lambda x: x[x.rindex('/')+1:])
    playlist_view_ids = playlist_view_ids.tolist()
    playlist_target_uris = df['target_uri'].dropna()
    playlist_target_uris = playlist_target_uris[playlist_target_uris.str.contains('playlist')]
    playlist_target_ids = playlist_target_uris.apply(lambda x: x[x.rindex('/')+1:] if '/' in x else x[x.rindex(':')+1:])
    playlist_target_ids = playlist_target_ids.dropna().tolist()
    playlist_ids = list(set(playlist_view_ids + playlist_target_ids))

    # get playlist info from ids
    if request:
        playlist_info = GET_playlist_info(
            playlist_ids, save_path=f'./processed_data/{subject_nr}/playlist_info_KmInteraction_{subject_nr}.json')
    else:
        file = open(f'./processed_data/{subject_nr}/playlist_info_KmInteraction_{subject_nr}.json')
        playlist_info = json.load(file)

    # add playlist title and description to dataframe
    view_titles = df['view_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'title', uri))
    view_descriptions = df['view_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'description', uri))
    target_titles = df['target_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'title', uri))
    target_descriptions = df['target_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'description', uri))

    titles = []
    descriptions = []
    for i in range(len(df)):
        titles.append(
            target_titles.iloc[i] if not target_titles.iloc[i] == '' else view_titles.iloc[i])
        descriptions.append(
            target_descriptions.iloc[i] if not target_descriptions.iloc[i] == '' else view_descriptions.iloc[i])
    df['title'] = titles
    df['description'] = descriptions
 
    contained_playlists = list(set(df['title'].tolist()))
    print(f"File contains information on the following playlists:\n {contained_playlists}")

    # save dataframe as csv
    df.to_csv(f'./processed_data/{subject_nr}/kmInteraction_playlists_{subject_nr}.csv')


def process_ParadoxCampaignOptimizerEvent(subject_nr ,request=False):
    """
    Processes data from ParadoxCampaignOptimizerEvent.json for playlist analysis. 
    See read_ParadoxCampaignOptimizerEvent() for more information on the contained data.

    Args:
        subjectNr(str):     Number assigned to the subject
        request(boolean):   True when playlist info needs to be requested, False otherwise
    """
    df = read_ParadoxCampaignOptimizerEvent()

    # check values (print one of the variables to see which values the data field contains)
    business_objectives = list(set(df.business_objective.tolist()))
    channels = list(set(df.channel.tolist()))
    action_urls = list(set(df.action_url.tolist()))
    action_url_types = list(set(df.action_url_type.tolist()))
    
    # get row with playlist url
    df_playlist = df.dropna()
    df_playlist = df_playlist[df_playlist['action_url'].str.contains('playlist')]
    playlist_ids = df_playlist['action_url'].apply(lambda uri: uri[uri.rindex(':')+1:])
    playlist_ids = list(set(playlist_ids.tolist()))
    
    # get playlist info from ids
    if request:
        playlist_info = GET_playlist_info(
            playlist_ids, save_path=f'./processed_data/{subject_nr}/playlist_info_ParadoxCampaignOptimizerEvent.json')
    else:
        file = open('./processed_data/{subject_nr}/playlist_info_ParadoxCampaignOptimizerEvent.json')
        playlist_info = json.load(file)

    print(playlist_info[playlist_ids[0]]['name'])


def process_PartnerNaturalLanguageAction(subject_nr ,request=False):
    """
    Processes data from PartnerNaturalLanguageAction.json for playlist analysis. 
    See read_PartnerNaturalLanguageAction() for more information on the contained data.

    Args:
        subjectNr(str):     Number assigned to the subject
        request(boolean):   True when playlist info needs to be requested, False otherwise
    """
    df = read_PartnerNaturalLanguageAction()

    # check values (print one of the variables to see which values the data field contains)
    context_urls = list(set(df.context_url.tolist()))
    
    # collect playlist entries
    df = df[df['context_url'].str.contains('playlist')]
    playlist_ids = df['context_url'].apply(lambda uri: uri[uri.rindex(':')+1:])
    playlist_ids = list(set(playlist_ids.tolist()))

    # get playlist info from ids
    if request:
        playlist_info = GET_playlist_info(
            playlist_ids, save_path='./processed_data/{subject_nr}/playlist_info_PartnerNaturalLanguageAction.json')
    else:
        file = open(f'./processed_data/{subject_nr}/playlist_info_PartnerNaturalLanguageAction.json')
        playlist_info = json.load(file)

    # add playlist title and description to dataframe
    df['title'] = df['context_url'].apply(
        lambda uri: query_playlist_data(playlist_info, 'title', uri))
    df['description'] = df['context_url'].apply(
        lambda uri: query_playlist_data(playlist_info, 'description', uri))

    contained_playlists = list(set(df['title'].tolist()))
    print(f"File contains information on the following playlists:\n {contained_playlists}")

    # save dataframe as csv
    df.to_csv(f'./processed_data/{subject_nr}/PartnerNaturalLanguageAction.csv')


def process_PartnerNaturalLanguageIntentResolution(subject_nr='' ,request=False):
    """
    Processes data from PartnerNaturalLanguageIntentResolution.json for playlist analysis. 
    See read_PartnerNaturalLanguageIntentResolution() for more information on the contained data.

    Args:
        subjectNr(str):     Number assigned to the subject
        request(boolean):   True when playlist info needs to be requested, False otherwise
    """
    df = read_PartnerNaturalLanguageIntentResolution()
    
    # check values (print one of the variables to see which values the data field contains)
    original_uris = list(set(df.original_uri.tolist()))
    utterances = list(set(df.utterance.tolist()))
    final_uris = list(set(df.final_uri.tolist()))
    
    # collect playlist entries
    df = df[df['final_uri'].str.contains('playlist')]
    playlist_ids = df['final_uri'].apply(lambda uri: uri[uri.rindex(':')+1:])
    playlist_ids = list(set(playlist_ids.tolist()))

    # get playlist info from ids
    if request:
        playlist_info = GET_playlist_info(
            playlist_ids, save_path=f'./processed_data/{subject_nr}/playlist_info_PartnerNaturalLanguageIntentResolution.json')
    else:
        file = open(f'./processed_data/{subject_nr}/playlist_info_PartnerNaturalLanguageIntentResolution.json')
        playlist_info = json.load(file)

    # add playlist title and description to dataframe
    df['title'] = df['final_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'title', uri[uri.rindex(':')+1:]))
    df['description'] = df['final_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'description', uri[uri.rindex(':')+1:]))

    contained_playlists = list(set(df['title'].tolist()))
    print(f"File contains information on the following playlists:\n {contained_playlists}")

    # save dataframe as csv
    df.to_csv(f'./processed_data/{subject_nr}/PartnerNaturalLanguageIntentResolution_playlists.csv')


def process_PlaybackFromDeeplink(subject_nr='' ,request=False):
    """
    Processes data from PlaybackFromDeeplink.json for playlist analysis. 
    See read_PlaybackFromDeeplink() for more information on the contained data.

    Args:
        subjectNr(str):     Number assigned to the subject
        request(boolean):   True when playlist info needs to be requested, False otherwise
    """
    df = read_PlaybackFromDeeplink()
    
    # check values (print one of the variables to see which values the data field contains)
    entity_uris = list(set(df.entity_uri.tolist()))
    context_uris = list(set(df.context_uri.tolist()))
    
    playlist_ids = df['context_uri'].apply(lambda uri: uri[uri.rindex(':')+1:])
    playlist_ids = list(set(playlist_ids.tolist()))

    # get playlist info from ids
    if request:
        playlist_info = GET_playlist_info(
            playlist_ids, save_path=f'./processed_data/{subject_nr}/playlist_info_PlaybackFromDeeplink.json')
    else:
        file = open(f'./processed_data/{subject_nr}/playlist_info_PlaybackFromDeeplink.json')
        playlist_info = json.load(file)

    # add playlist title and description to dataframe
    df['title'] = df['context_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'title', uri[uri.rindex(':')+1:]))
    df['description'] = df['context_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'description', uri[uri.rindex(':')+1:]))

    contained_playlists = list(set(df['title'].tolist()))
    print(f"File contains information on the following playlists:\n {contained_playlists}")

    # save dataframe as csv
    df.to_csv(f'./processed_data/{subject_nr}/PlaybackFromDeeplink_playlists.csv')


def process_ReleaseRadarServedRecs(subjectNr='' ,request=False):
    """
    Processes data from ReleaseRadarServedRecs.json for playlist analysis. 
    See read_ReleaseRadarServedRecs() for more information on the contained data.

    Args:
        subjectNr(str):     Number assigned to the subject
        request(boolean):   True when playlist info needs to be requested, False otherwise
    """

    df = read_ReleaseRadarServedRecs()

    # check values (print one of the variables to see which values the data field contains)
    made_for_user = list(set(df.made_for_user.tolist()))
    context_uri = list(set(df.context_uri.tolist()))[0] # probably just ReleaseRadar
    
    playlist_id = context_uri[context_uri.rindex(':')+1:]

    # get playlist info from ids
    if request:
        playlist_info = GET_playlist_info(
            [playlist_id], save_path=f'./processed_data/{subject_nr}/playlist_info_ReleaseRadarServedRecs.json')
    else:
        file = open('./processed_data/{subject_nr}/playlist_info_ReleaseRadarServedRecs.json')
        playlist_info = json.load(file)

    # add playlist title and description to dataframe
    df['title'] = df['context_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'title', uri[uri.rindex(':')+1:]))
    df['description'] = df['context_uri'].apply(
        lambda uri: query_playlist_data(playlist_info, 'description', uri[uri.rindex(':')+1:]))

    contained_playlists = list(set(df['title'].tolist()))
    print(f"File contains information on the following playlists:\n {contained_playlists}")

    # save dataframe as csv
    df.to_csv(f'./processed_data/{subject_nr}/ReleaseRadarServedRecs_playlists.csv')


def process_VoiceContentCreated(subjectNr='', request=False):
    """
    Processes data from VoiceContentCreated.json for playlist analysis. 
    See read_VoiceContentCreated() for more information on the contained data.

    Args:
        subjectNr(str):     Number assigned to the subject
        request(boolean):   True when playlist info needs to be requested, False otherwise
    """
    df = read_VoiceContentCreated()
    df = df[df['content_uri'].str.contains('playlist')]
    print(df[['timestamp_utc', 'content_uri']])

    # check values (print one of the variables to see which values the data field contains)
    content_uris = list(set(df.content_uri.tolist()))
    filtered = list(set(df.filtered.tolist()))


######################### REQUEST PLAYLIST INFORMATION #########################

def GET_playlist_info(playlist_ids, save_path):
    api_token = get_api_token(scope='playlist-read-collaborative')
    playlist_dict = {}

    for id in playlist_ids:
        playlist_info = request_playlist_info(id, api_token)
        playlist_dict[id] = playlist_info

    with open(save_path, "w") as outfile:
        json.dump(playlist_dict, outfile, indent=4)

    return playlist_dict


def get_playlist_info(file_name, save_path):

    if file_name == 'sporty':
        playlist_df = read_SportyFormatlistRequest()
        playlist_ids = playlist_df['playlist_id'].tolist()

    elif file_name == 'bassline':
        df = read_BasslineRequests()
        df['content'] = df['content'].apply(lambda x: json.loads(x))
        df['content'] =  df['content'].apply(lambda x: list(x.values()) if isinstance(x, dict) else x)
        
        nested_uris = df['content'].tolist()
        uris = []
        
        def flatten(nested_list):
            for i in nested_list:
                if type(i) == list:
                    flatten(i)
                else:
                    uris.append(i)

        flatten(nested_uris)
        
        playlist_uris = list(filter(lambda x: isinstance(x, str) and 'spotify:playlist' in x, uris))
        playlist_ids = [x[x.rindex(':')+1:] for x in playlist_uris]
        playlist_ids = list(set(playlist_ids))

    else:
        print('Invalid file name! Must be "sporty" or "bassline"')
        return

    api_token = get_api_token(scope='playlist-read-collaborative')
    playlist_dict = {}

    for id in playlist_ids:
        playlist_info = request_playlist_info(id, api_token)
        playlist_dict[id] = playlist_info

    with open(save_path, "w") as outfile:
        json.dump(playlist_dict, outfile, indent=4)


def query_playlist_data(playlist_info, query, uri):

    if not uri:
        return ''
    elif '/' in uri:
        id = uri[uri.rindex('/')+1:]
    elif ':' in uri:
        id = uri[uri.rindex(':')+1:]
    else:
        return ''

    if query == 'title': 
        try:
            title = playlist_info[id]['name']
            return title
        except:
            return ''
    elif query == 'description': 
        try:
            description = playlist_info[id]['description']
            return description
        except:
            return ''
    else:
        print(f'Query {query} invalid!')
        return None


def build_df(file_name, subject_nr, save_path):

    if file_name == 'sporty':
        playlist_df = read_SportyFormatlistRequest()

        # open playlist info
        file = open(f"./processed_data/{subject_nr}/playlist_info.json")
        playlist_info = json.load(file)

        # get playlist title
        playlist_df["title"] = playlist_df["playlist_id"].apply(lambda id: query_playlist_data(playlist_info, "title", id))
        print(list(set(playlist_df["title"].tolist())))

        # save as csv
        playlist_df.to_csv(save_path)

    elif file_name == 'bassline':
        playlist_df = read_BasslineRequests()
        playlist_df['content'] = playlist_df['content'].apply(lambda x: x if 'spotify:playlist' in x else None)
        playlist_df = playlist_df.dropna()
        print(playlist_df)  # shows that playlists stem from only 2 timestamps

        file = open(f"./processed_data/{subject_nr}/playlist_info_BasslineRequests.json")
        playlist_info = json.load(file)
        title = [entry['name'] for entry in list(playlist_info.values())]
        description = [entry['description'] for entry in list(playlist_info.values())]
        owner = [entry['owner']['display_name'] for entry in list(playlist_info.values())]
        collaborative = [entry['collaborative'] for entry in list(playlist_info.values())]

        playlist_dict = {
            'title': title,
            'description': description,
            'owner': owner,
            'collaborative': collaborative
        }
        playlist_df = pd.DataFrame(playlist_dict)
        playlist_df.to_csv(f'./processed_data/{subject_nr}/basslineRequests_playlists.csv')

    else:
        print('Invalid file name! Must be "sporty" or "bassline"')


def calculate_session_length(session):
    return session.max() - session.min()
    

def analyse_playlists(subject_nr, name, titles=[], plots=[]):
    """
    Analyses playlist data based on the KmInteraction and PartnerNaturalLanguageIntentResolution
    data files. 
    """

    # get data
    interaction_df = read_UI_playlists(subject_nr)[['timestamp_utc', 'title']]
    interaction_df['selection_mode'] = 'UI'
    voice_df = read_voice_playlists(subject_nr)[['timestamp_utc', 'title']]
    voice_df['selection_mode'] = 'voice'
    df = pd.concat([interaction_df, voice_df])
    df.sort_values(by='timestamp_utc')

    # plots with all playlists
    if 'all_heatmap' in plots:
        plot_playlist_heatmap(df, title='All Playlists', save_path=f'./output/{subject_nr}/heatmap_all_playlists.jpg')
    elif 'all_weekday' in plots:
        plot_count_per_weekday(df, title='All Playlists', save_path=f'./output/{subject_nr}/weekdays_all_playlists.jpg')
    elif 'all_daytime' in plots:
        plot_count_per_daytime(df, title='All Playlists', save_path=f'./output/{subject_nr}/daytime_all_playlists.jpg')

    # plots for selection mode
    elif 'UI' in plots:
        df_mode = df[df['selection_mode'] == 'UI']
        plot_playlist_heatmap(df_mode, title=name, save_path=f'./output/{subject_nr}/heatmap_{name}.jpg')
    elif 'voice' in plots:
        df_mode = df[df['selection_mode'] == 'voice']
        plot_playlist_heatmap(df_mode, title=name, save_path=f'./output/{subject_nr}/heatmap_{name}.jpg')
    
    # plots for specific playlists
    df = df[df['title'].isin(titles)]
    # df = df[df['selection_mode'] == 'voice']
    
    if 'heatmap' in plots:
        plot_playlist_heatmap(df, title=name, save_path=f'./output/{subject_nr}/heatmap_{name}.jpg')
    elif 'weekday' in plots:
        plot_count_per_weekday(df, title=name, save_path=f'./output/{subject_nr}/weekdays_{name}.jpg')
    elif 'daytime' in plots:
        plot_count_per_daytime(df, title=name, save_path=f'./output/{subject_nr}/daytime_{name}.jpg')



def analyse_editorial_playlists(playlist, plots=[], subject_nr):
    data = read_playlist_df(f'./processed_data/{subject_nr}/editorial_playlists.csv')
    # playlist_data = data[data['title'] == playlist]
    playlist_data = data

    # create listening sessions
    playlist_data.sort_values(by='timestamp', inplace=True)
    print(playlist_data.head(20))
    # playlist_data_sessions = create_sessions(playlist_data, interval=1*60*60)
    # playlist_data_sessions = playlist_data.drop_duplicates(subset=['session_id'])

    # TODO: average length of session
    # session_lengths = playlist_data_sessions.groupby('session_id').timestamp.agg(calculate_session_length)
    # print(session_lengths)

    # print(playlist_data_sessions[playlist_data_sessions['session_id']==1])

    print(f"\nPlaylist occurence info: '{playlist}'")
    print(f" -- found {playlist_data.shape[0]} entries")
    # print(f" -- listened to during {playlist_data_sessions.shape[0]} sessions")

    if 'heatmap' in plots:
        plot_playlist_heatmap(playlist_data, playlist)
    if 'per_date' in plots:
        plot_count_per_date(playlist_data, playlist)


def analyse_album_occurrence(album_title, plots=[], subject_nr):
    df = read_endsong(root=f'./raw_data/{subject_nr}', file_count=9)
    album_df = df[df['album_name']==album_title]
    album_df['offline_datetime'] = album_df['offline_timestamp'].apply(unix_to_date)
    album_df = album_df.drop('timestamp', axis=1)
    album_df.rename(columns={
        'offline_datetime': 'timestamp'}, inplace=True)   
    album_df['timestamp'] = pd.to_datetime(album_df.timestamp).dt.tz_localize(None)

    # remove outliers (nach März 2019) -> Handle them seperately
    # outliers = album_df[album_df['timestamp'] > datetime(2019, 3, 31)]
    # outliers.sort_values(by='timestamp', inplace=True)
    # album_df = album_df[album_df['timestamp'] <= datetime(2019, 3, 31)]

    if 'count_per_date' in plots:
        plot_count_per_date(album_df, album_title)
    if 'length_per_date' in plots:
        plot_length_per_date(album_df, album_title)
    if 'heatmap' in plots:
        plot_playlist_heatmap(album_df, album_title)
    if 'count_per_weekday' in plots:
        plot_count_per_weekday(album_df, album_title)
    if 'length_per_weekday' in plots:
        plot_length_per_weekday(album_df, album_title)

    # mean listening time per day
    length_per_day = album_df.groupby(album_df['timestamp'].dt.date)['ms_played'].sum()
    mean_length_per_day = length_per_day.mean() / 3600000
    std = length_per_day.std() / 3600000
    print(f"\nMean listening time per day: {mean_length_per_day} hours (std of {std})")

    # create sessions    
    album_df.sort_values(by='timestamp', inplace=True)
    album_df = create_sessions(album_df, interval=0.5*60*60) # 30 minutes
    session_lengths = album_df.groupby('session_id').timestamp.agg(calculate_session_length)
    mean_session_length = session_lengths.mean()
    print(f"\nMean session length: {mean_session_length} hours")

    # playlist_data_sessions = playlist_data.drop_duplicates(subset=['session_id'])


if __name__ == "__main__":
    # get_playlist_info()
    # build_df('bassline', '')
    # analyse_playlist_occurrence('WOR K  OUT')
    # analyse_album_occurrence(album_title="45 Thoughtful Rain Tracks")

    # process_KmInteraction(subject_nr='001')
    analyse_playlists(
        '001', titles=['Release Radar', 'Happy Mix', 'Pop Mix', 'Upbeat Mix', 'Latin Mix', 'K-Pop Mix' ], plots=[], name='New')
