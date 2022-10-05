import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.cm as cmx
import matplotlib.ticker as plticker
import folium
import calendar
import webbrowser
import pandas as pd
import seaborn as sns
from math import pi
from folium.plugins import HeatMapWithTime

sns.set(font='georgia')
sns.set_style('ticks')


def plot_listening_behavior(data, time_window=None, cat=None, cat_values=None):
    """
    Plots given time data as scatter plot (daytime x date)

    Args:
        df: Dataframe containing the time data
        time_window: Tuple of datetimes to select time window
        event_types: List of strings containing to select the events
    """

    fig, axes = plt.subplots(1, 1)

    # select time window
    if time_window:
        selected_data = data.loc[(data['timestamp'] >= time_window[0])
                                 & (data['timestamp'] <= time_window[1])]
    else:
        selected_data = data

    if not cat_values:
        cat_values = list(set(selected_data[cat].tolist()))

    # set up axes data
    x = selected_data['timestamp'].apply(lambda x: x.date())
    y = selected_data['timestamp'].apply(lambda x: x.hour+x.minute/60.0)

    # print(x['timestamp'].head())

    # create color map
    z = range(1, len(cat_values))
    hot = plt.get_cmap('hot')
    cNorm = colors.Normalize(vmin=0, vmax=len(cat_values))
    scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=hot)

    # plot categories
    for i in range(len(cat_values)):
        indx = selected_data[cat] == cat_values[i]
        plt.scatter(x[indx], y[indx], s=2,
                    color=scalarMap.to_rgba(i), label=cat_values[i])

    axes.invert_yaxis()
    plt.xticks(rotation=90)
    plt.xlabel('Date')
    plt.ylabel('Time')
    plt.legend(loc='upper left')
    plt.title("Spotify Timeline")
    plt.show()


def plot_monthly_listening(data):
    plt.style.use('ggplot')
    data.plot(kind='bar',figsize=(12,7), color='blue', alpha=0.5)

    # title and x,y labels
    plt.title('Monthly Number of Tracks', fontsize=20)
    plt.xlabel('Month',fontsize=16)
    plt.ylabel('Number of tracks played',fontsize=16)

    plt.show()


def plot_weekday_listening(data):
    data.plot(kind='bar', figsize=(12, 7), color='magenta', alpha=0.5)

    # title and x, y labels
    plt.title('Number of Tracks per Weekday')
    plt.xlabel('Weekday',fontsize=16)
    plt.ylabel('Number of tracks played',fontsize=16)

    plt.show()


def plot_day_listening(data, mondays, fridays):
    data.plot(figsize=(13, 8), color='blue')

    # Title, x label and y label
    plt.title('Number of Tracks per Day', fontsize=20)
    plt.xlabel('Date',fontsize=16)
    plt.ylabel('Number of Tracks',fontsize=16)
    # plt.legend()

    plt.show()


def plot_hour_listening(data):
    data.plot(kind='bar', figsize=(12, 7), color='orange', alpha=0.5)

    # Title, x label and y label
    plt.title('Number of Tracks per Hour', fontsize=20)
    plt.xlabel('Hour',fontsize=16)
    plt.ylabel('Number of Tracks',fontsize=16)

    plt.show()


def plot_weekday_hour_listening(data):
    data.unstack().plot(kind='barh', figsize=(16,26))

    # title and x,y labels
    plt.legend(labels=[calendar.day_name[x] for x in range(0,7)],fontsize=16)
    plt.title('Number of Tracks per Weekday and Hour',fontsize=20)
    plt.xlabel('Number Tracks',fontsize=16)
    plt.ylabel('Hour',fontsize=16)

    plt.show()


def plot_location_heatmap(lats, lons, timestamps):
    average_coords = [(min(lats) + max(lats)) / 2,
                      (min(lons) +  max(lons)) / 2]

    map = folium.Map(location=average_coords, zoom_start=12)

    time_list = [[] for _ in range(len(timestamps))]
    for lat, lon, time in zip(lats, lons, timestamps):
        time_list[time].append([lat,lon]) 

    # Labels indicating the hours
    index = [str(i) for i in timestamps]

    # Instantiate a heat map wiht time object for the car accidents
    HeatMapWithTime(hour_list, index).add_to(map)

    map.showMap()

def plot_clustered_location_map(lats, lons):
    pass


def plot_location_map(lats, lons):

    average_coords = [(min(lats) + max(lats)) / 2,
                      (min(lons) +  max(lons)) / 2]

    map = folium.Map(location=average_coords, zoom_start=12)
    for lat, lng in zip(lats, lons):
        folium.features.CircleMarker(
            [lat, lng],
            radius=3,
            color='red',
            fill=True,
            fill_color='darkred',
            fill_opacity=0.6
        ).add_to(map)

    map.save("./map.html")

    firefox = webbrowser.Mozilla("C:\\Program Files\\Mozilla Firefox\\firefox.exe")
    firefox.open("file:///C:/Users/tj-we/OneDrive/Dokumente/Studium/Master/sem_3/data_ethics/defySpotify/map.html")



def plot_single_location(lat, lon, zoom=12):
    map = folium.Map(location=(lat, lon), zoom_start=zoom)
    folium.features.CircleMarker(
            [lat, lon],
            radius=3,
            color='red',
            fill=True,
            fill_color='darkred',
            fill_opacity=0.6
        ).add_to(map)

    map.save("./map.html")
    webbrowser.open("./map.html")


def plot_all_locations_over_time(data):
    data = data[data['event_type']=='location']['timestamp'].dt.date.to_frame()
    data = data.drop_duplicates()
    data['recorded_location'] = 1

    data = data.set_index('timestamp')
    data = data.asfreq('D').fillna(0)

    # create plot
    fig = data.plot(kind="area")
    plt.tick_params(left=False, labelleft=False, bottom=False)
    plt.title(f'Overview Location Data')
    plt.savefig(f'./output/location_overview_subject_1.jpg')
    plt.show()


def plot_location_occurrence_over_time(data, ip_addr, time_window=None):
    # prepare data
    data = data[data['event_type']=='location']
    start_date, end_date = data['timestamp'].min().date(), data['timestamp'].max().date()
    data = data[data['ip_addr']==ip_addr]['timestamp'].dt.date.to_frame()
    data = data.drop_duplicates()
    data['visited'] = 1

    # set correct start and end date
    if not data['timestamp'].min() == start_date:
        new_row = pd.DataFrame({'timestamp': start_date, 'visited': 0}, index =[0])
        data = pd.concat([new_row, data]).reset_index(drop = True)
    if not data['timestamp'].max() == end_date:
        new_row = pd.DataFrame({'timestamp': end_date, 'visited': 0}, index=[data.index.max()+1])
        data = data.append(new_row)

    data = data.set_index('timestamp')
    data = data.asfreq('D').fillna(0)

    # create plot
    fig = data.plot(kind="area")
    plt.tick_params(left=False, labelleft=False, bottom=False)
    plt.title(f'Was at {ip_addr}')
    plt.savefig(f'./output/{ip_addr}_subject_1.jpg')
    plt.show()


def plot_location_over_months(data, ip_addr):
    # prepare data
    data = data[data['ip_addr'] == ip_addr][['ip_addr', 'event_type', 'timestamp']]
    location_per_month = data.groupby(data['timestamp'].dt.month).count()
    location_per_month = location_per_month.reindex(range(1, 13), fill_value=0) # make sure that all months are present
    location_per_month.index=[calendar.month_name[x] for x in range(1,13)]
    location_per_month = location_per_month['timestamp']
    print(location_per_month)

    # create plot
    plt.style.use('ggplot')
    location_per_month.plot(kind='bar',figsize=(12,7), color='blue', alpha=0.5)

    # title and x,y labels
    plt.title(f'Monthly presence at {ip_addr}', fontsize=20)
    plt.xlabel('Month',fontsize=16)
    plt.ylabel('',fontsize=16)

    plt.savefig(f'./output/{ip_addr}_over_months_subject_1.jpg')
    plt.show()


def plot_location_over_hours(data, ip_addr):
    # prepare data
    data = data[data['ip_addr'] == ip_addr][['ip_addr', 'event_type', 'timestamp']]
    location_per_hour = data.groupby(data['timestamp'].dt.hour).count()
    location_per_hour = location_per_hour['timestamp']

    # create plot
    plt.style.use('ggplot')
    location_per_hour.plot(kind='bar',figsize=(12,7), color='blue', alpha=0.5)

    # title and x,y labels
    plt.title(f'Hourly presence at {ip_addr}', fontsize=20)
    plt.xlabel('Hour',fontsize=16)
    plt.ylabel('',fontsize=16)

    plt.savefig(f'./output/{ip_addr}_over_hours_subject_1.jpg')
    plt.show()


############################### PLAYLISTS ###############################

def plot_playlist_heatmap(data, title, save_path):    
    # https://stackabuse.com/ultimate-guide-to-heatmaps-in-seaborn-with-python/

    data['timestamp_utc'] = pd.to_datetime(data['timestamp_utc'])

    plays_per_hour_day = data.groupby([ 
        data['timestamp_utc'].dt.floor('2H').dt.hour.rename('hour'),
        data['timestamp_utc'].dt.weekday]).count()
    plays_per_hour_day = plays_per_hour_day['timestamp_utc']

    # fill missing hours (TODO: do with in-built pandas function, not manually!)
    for x in [0, 2, 22]:
        for y in [0]:
            plays_per_hour_day[x, y] = 0
    plays_per_hour_day = plays_per_hour_day.sort_index(axis=0)

    print(plays_per_hour_day)

    ax = sns.heatmap(
        plays_per_hour_day.unstack().fillna(0), 
        annot=False,
        cmap='flare',
        linewidth=0.01,
        linecolor='lightgray'
    )
    ax.tick_params(left=False, bottom=False)
    ax.set_xticklabels([calendar.day_name[x][0] for x in range(0, 7)], fontsize=8)

    plt.title(title)
    plt.ylabel('Hour of Day')
    plt.xlabel('Day of Week')

    plt.savefig(save_path)
    plt.show()


def plot_count_per_date(data, playlist, save_path):
    plays_per_date = data.groupby(data['timestamp_utc'].dt.date).count()
    plays_per_date = plays_per_date['timestamp_utc']
    plays_per_date = plays_per_date.asfreq('D').fillna(0)
    plays_per_date.index = plays_per_date.index.date

    plt.style.use('ggplot')
    ax = plays_per_date.plot(kind='bar', color='blue', alpha=0.5)

    # title and x,y labels
    plt.title(f'All Playlists', fontsize=12)
    plt.xlabel('Date', fontsize=10)
    plt.ylabel('Play Count', fontsize=10)
    plt.tick_params(labelsize=8)
    loc_x = plticker.MultipleLocator(base=50)
    ax.xaxis.set_major_locator(loc_x)
    loc_y = plticker.MultipleLocator(base=50)
    ax.yaxis.set_major_locator(loc_y)
    plt.gcf().autofmt_xdate() # Rotations

    plt.savefig(save_path)
    plt.show()


def plot_count_per_weekday(data, title, save_path):
    data['timestamp_utc'] = pd.to_datetime(data['timestamp_utc'])

    plays_weekday = data.groupby(data['timestamp_utc'].dt.dayofweek).count()
    plays_weekday.index=[calendar.day_name[x][0] for x in range(0, 7)]
    plays_weekday = plays_weekday['timestamp_utc']

    plays_weekday.plot(kind='bar', color='magenta', alpha=0.5)

    # title and x, y labels
    plt.title(title)
    plt.xlabel('Day of Week')
    plt.ylabel('Play Count')
    plt.xticks(rotation='horizontal')

    plt.savefig(save_path)
    plt.show()


def plot_count_per_daytime(data, title, save_path):
    data['timestamp_utc'] = pd.to_datetime(data['timestamp_utc'])

    plays_per_daytime = data.groupby( 
        data['timestamp_utc'].dt.floor('2H').dt.hour.rename('Time of Day')).count()
    plays_daytime = plays_per_daytime['timestamp_utc']

    plays_daytime.plot(kind='bar', color='magenta', alpha=0.5)

    # title and x, y labels
    plt.title(title)
    plt.xlabel('Time of Day')
    plt.ylabel('Play Count')
    plt.xticks(rotation='horizontal')

    plt.savefig(save_path)
    plt.show()


def plot_length_per_date(data, album_title):
    length_per_date = data.groupby(data['timestamp'].dt.date)['ms_played'].sum()
    length_per_date = length_per_date.apply(lambda x: x/3600000)
    length_per_date = length_per_date.asfreq('D').fillna(0)
    length_per_date.index = length_per_date.index.date

    plt.style.use('ggplot')
    ax = length_per_date.plot(kind='bar', color='blue', alpha=0.5)

    # title and x,y labels
    plt.title(f'Listened to "{album_title}"')
    plt.xlabel('Date')
    plt.ylabel('Length [h]')
    plt.tick_params(labelsize=8)
    loc_x = plticker.MultipleLocator(base=50)
    ax.xaxis.set_major_locator(loc_x)
    # plt.gcf().autofmt_xdate() # Rotations

    plt.savefig(f'./output/lukas/{album_title}_length_per_date_lukas.jpg')
    plt.show()


def plot_length_per_weekday(data, album_title):
    length_per_date = data.groupby(data['timestamp'].dt.date)['ms_played'].sum()
    length_per_date = length_per_date.apply(lambda x: x/3600000)
    length_per_date.index = pd.to_datetime(length_per_date.index)

    mean_per_weekday = length_per_date.groupby(length_per_date.index.dayofweek).mean()
    mean_per_weekday.index=[calendar.day_name[x] for x in range(0, 7)]

    mean_per_weekday.plot(kind='bar', color='magenta', alpha=0.5)

    # title and x, y labels
    plt.title(album_title)
    plt.xlabel('Weekday')
    plt.ylabel('Mean listening time [h]')

    plt.savefig(f'./output/lukas/{album_title}_mean_length_per_weekday_lukas.jpg')
    plt.show()


############################### GENRES ###############################

def plot_genres(subject, genres_df):
    limit = 25
    max_values = genres_df[:limit]

    fig = plt.figure(figsize = (10, 5))
    plt.bar(max_values['genre'], max_values['count'])
    plt.xlabel('Genres')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('No. of tracks in the streaming history')
    plt.title('Top ' + str(len(max_values['genre'])) + ' genres of all streamed tracks')
    plt.savefig('output/' + subject + '/genres.png', bbox_inches='tight')


############################### Inferences ###############################

def plot_inferences(values, path):
    #TODO this is so disgusting I'm gonna cry
    labels = ['News', 'History', 'Books', 'Studying or\nfocusing', 'Education', 'Science &\nMedicine', 'Tech', 'Business', 'Commuting', 'In-car\nlistening', 'Travel', 'Culture &\n Society', 'Love &\nDating', 'Parenting', 'Cooking', 'Health &\nLifestyle', 'Fitness', 'Running', 'Sports &\nRecreation', 'DIY\nHobbies\n& Crafts', 'Partying', 'Comedy', 'Gaming', 'Theater', 'TV & Film', 'Podcasts']

    n_categories = len(labels)

    angles = [n / float(n_categories) * 2 * pi for n in range(n_categories)]
    angles += angles[:1]

    ax = plt.subplot(111, polar=True)
    plt.xticks(angles[:-1], labels, color='grey', size=8)
    ax.set_rlabel_position(0)
    plt.yticks([0,50], ['',''], color='grey', size=7)
    plt.ylim(0,40)
    ax.spines['start'].set_color('none')
    ax.spines['polar'].set_color('none')
    ax.plot(angles, values, linewidth=5, linestyle='solid')
    ax.fill(angles, values, 'b', alpha=0.25)
    plt.savefig(path, bbox_inches='tight')

    """
    TODO make a box plot for all subjects + include gender + age
        (x:age, y:cat, left:male, right:female)
    """


############################### Mood ###############################

def features_sns(features, avg_features, labels, title):
    #fig, ax = plt.subplots(figsize = (10,3))
    fig, ax = plt.subplots(figsize = (25,3))
    n = len(labels)
    x = [x for x in range(n)]

    for feature in features:
        y = [getattr(step, feature) for step in avg_features]
        fig = sns.lineplot(x=x,y=y, label=feature, linewidth=6, alpha=.7, marker='o', markersize=15)
    
    ax.set_xticks([x for x in range(n)])
    ax.set_xticklabels(labels=labels, rotation=45, ha='right', size=10)

    for tick in ax.yaxis.get_major_ticks():
                tick.label.set_fontsize(10) 

    leg = ax.legend(loc = 'upper left', bbox_to_anchor=(.85,1), prop={'size': 10})

    for line in leg.get_lines():
        line.set_linewidth(10)
    
    ax.set_title(title, size = 15, pad = 10, fontname = 'sans-serif')

    return ax


def plot_endsong_mood(months, avg_features_months, path):
    month_labels = [f'{month[1]}/{month[0]}' for month in months]
    month_labels_short = [m[:3]+m[-2:] for m in month_labels]

    title = 'Normalized mood according to audio features of streaming history'

    features_sns(['valence', 'energy'], avg_features_months, month_labels_short, title)
    plt.ylim([-2, 2])
    plt.xlim([0, len(month_labels_short)-1])
    plt.ylabel('Value')
    plt.xlabel('Month and year')
    plt.axhline(y=0, color='b', linestyle='--')
    plt.savefig(path, bbox_inches='tight')


def plot_playlist_mood(names, avg_features_playlist, path):
    playlist_labels = [f'{playlist}' for playlist in names]
    title = 'Normalized mood according to audio features of playlists'

    features_sns(['valence', 'energy'], avg_features_playlist, playlist_labels, title)
    plt.ylim([-1.5, 1.5])
    plt.xlim([0, len(names)-1])
    plt.ylabel('Value')
    plt.xlabel('Playlist')
    plt.axhline(y=0, color='b', linestyle='--')
    plt.savefig(path, bbox_inches='tight')