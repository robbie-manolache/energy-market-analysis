import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime as dt
import pandas as pd
from __common__ import user_choice

def __load_json__(file):
    """
    """
    try:
        with open(file) as rf:
            data = json.load(rf) 
        return data 
    except:
        return {}

def __get_links__(url, base_url):
    """
    """
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    link_dict = {}
    ignore = ['[To Parent Directory]', 'DUPLICATE']
    for link in soup.findAll('a'):
        if link.text in ignore :
            pass
        else:
            link_dict[link.text] = base_url+link.get('href')
    return link_dict

def __gen_file_name__(data_keys):
    """
    """
    name = set(map(lambda x: '-'.join(x.split('_')[:-2]).lower(),
                   data_keys))
    name = list(name)
    if len(name) > 1:
        print("More than one file type detected!")
        return None
    else:
        return name[0]+'.csv'

def __gen_tracker_df__(data_files, data_keys):
    """
    """
    data_dict = {
        'TIMESTAMP': pd.to_datetime(list(map(lambda x: 
                                    x.split('_')[-2], 
                                    data_keys))),
        'VERSION': list(map(lambda x: 'V'+x.split('_')[-1][:-4], 
                                      data_keys)),
        'DOWNLOADED': False, 
        'DOWNLOAD_DATE': pd.to_datetime('19000101'),                          
        'URL': list(data_files.values())
    }
    return pd.DataFrame(data_dict)

def __merge_trackers__(old_df, new_df):
    """
    Add anything from new_df that is not in old_df already!
    """
    merge_cols = ['TIMESTAMP', 'VERSION']
    old_temp = old_df.copy()[merge_cols]
    new_temp = new_df.copy()[merge_cols]
    merge_df = pd.merge(old_temp, new_temp, on=merge_cols,
                        how='outer', indicator=True)
    old_rows = merge_df[merge_df["_merge"]=="left_only"]
    old_rows = old_rows[merge_cols].merge(old_df, on=merge_cols)
    # keep only old rows that have been downloaded
      # else they need to extracted from different URL
    old_rows = old_rows[old_rows.DOWNLOADED]
    return pd.concat([old_rows, new_df], ignore_index=True)

class NEM_tracker:
    """
    Class object for tracking various resources on NEM website.
    Can potentially be extended to other websites too. 
    """

    def __init__(self, data_dir, base_url="http://nemweb.com.au"):
        """
        """
        self.data_dir = data_dir
        self.base_url = base_url
        self.resource_path = os.path.join(data_dir, 'resources.json')
        self.resources = __load_json__(self.resource_path)
        self.selected_resource = None
        self.tracker_dir = os.path.join(data_dir, 'resource_trackers')
        if os.path.isdir(self.tracker_dir):
            pass
        else:
            os.makedirs(self.tracker_dir)

    def update_resource(self, resource):
        """
        """
        url = self.base_url + resource
        data_files = __get_links__(url, self.base_url)
        data_keys = list(data_files.keys())
        file_name = __gen_file_name__(data_keys)        
        self.resources[resource] = {
            'url': url,
            'tracker_file': file_name,
            'last_update': dt.now().strftime('%Y-%m-%d-%H:%M:%S')
        }
        with open(self.resource_path, 'w') as wf:
            json.dump(self.resources, wf)
        new_df = __gen_tracker_df__(data_files, data_keys)       
        file_path = os.path.join(self.tracker_dir, file_name)
        if file_name in os.listdir(self.tracker_dir):
            old_df = pd.read_csv(file_path, parse_dates=[
                'TIMESTAMP', 'DOWNLOAD_DATE'
            ])
            tracker_df = __merge_trackers__(old_df, new_df)
        else:
            tracker_df = new_df
        tracker_df.to_csv(file_path, index=False)
    
    def add_resources(self, resources):
        """
        """
        if type(resources) is not list:
            resources = [resources]
        else:
            pass
        for resource in resources:
            self.update_resource(resource)


    def bulk_update(self):
        """
        """
        for resource in self.resources.keys():
            self.update_resource(resource)

    def select_resource(self):
        """
        """
        res_list = self.resources.keys()
        self.selected_resource = user_choice(res_list)
