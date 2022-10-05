import pandas as pd
import json

from utils.parse_data import read_endsong, read_locations
from utils.plots import plot_location_map, plot_location_heatmap

def process_listening_history(subject_nr, nr_files, request=False):

    df = read_endsong(f'./raw_data/{subject_nr}', nr_files)
    df = df[['timestamp', 'ip_addr']]
    
    ip_addrs = list(set(df['ip_addr'].tolist()))

    if request:
        coordinate_info = get_coordinates(ip_addrs)
    else:
        file = open(f'./processed_data/{subject_nr}/location_data_{subject_nr}.json')
        coordinate_info = json.load(file)

    df['coordinates'] = df['ip_addr'].apply(lambda ip: coordinate_info[ip]['coords'])
    df['lat'] = df['ip_addr'].apply(lambda ip: coordinate_info[ip]['lat'])
    df['lon'] = df['ip_addr'].apply(lambda ip: coordinate_info[ip]['lon'])

    # save dataframe as csv
    df.to_csv(f'./processed_data/{subject_nr}/locations.csv')
    

def get_coordinates(ip_addrs, save_path):
    """
    Args:
        ip_addrs(list): ip addresses from spotify data
    """

    location_df = pd.DataFrame(ip_addrs, columns=['ip_addr'])
    location_df['coordinates'] = location_df['ip_addr'].apply(
        lambda x: get_ip_info(x)['loc'])

    coordinates = [get_ip_info(ip) for ip in ip_addrs]
    
    location_df = pd.DataFrame({'coordinates': coordinates})
    location_df[['lat','lon']] = location_df['coordinates'].str.split(',',expand=True)
    location_dict = {a: {'lat': b, 'lon': c, 'coords': d} for (a, b, c, d) in zip(
        location_df['ip_addr'].tolist(), 
        location_df['lat'].tolist(), 
        location_df['lon'].tolist(),
        location_df['coordinates'].tolist())} 

    with open(save_path, "w") as outfile:
        json.dump(location_dict, outfile, indent=4)

    return location_dict
    

def analyse_locations(subject_nr):
    df = read_locations(subject_nr)
    # plot_location_map(df['lat'].tolist(), df['lon'].tolist())
    plot_location_heatmap(df['lat'].tolist(), df['lon'].tolist(), df['timestamp'].tolist())

if __name__ == "__main__":
    analyse_locations('002')