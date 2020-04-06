import os
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd

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
    for link in soup.findAll('a'):
        if link.text == '[To Parent Directory]':
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
        'UPLOAD_DATE': pd.to_datetime(list(map(lambda x: 
                                      x.split('_')[-1][:-4], 
                                      data_keys))),
        'DOWNLOADED': False, 
        'DOWNLOAD_DATE': pd.to_datetime('19000101'),                          
        'URL': list(data_files.values())
    }
    return pd.DataFrame(data_dict)

def __merge_trackers__(old_df, new_df):
    """
    Add anything from new_df that is not in old_df already!
    """
    merge_cols = ['TIMESTAMP', 'UPLOAD_DATE']
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
        self.tracker_dir = os.path.join(data_dir, 'resource_trackers')
        if os.path.isdir(self.tracker_dir):
            pass
        else:
            os.makedirs(self.tracker_dir)

    def _resource_dict_entry(self, resource, url):
        """
        """
        self.resources[resource] = {
            'url': url,
            'data_files': __get_links__(url, self.base_url), 
            'tracker_file': ''
        }
        with open(self.resource_path, 'w') as wf:
            json.dump(self.resources, wf)
    
    def _resource_csv_tracker(self, resource):
        """
        """
        data_files = self.resources[resource]['data_files']
        data_keys = list(data_files.keys())
        new_df = __gen_tracker_df__(data_files, data_keys)
        file_name = __gen_file_name__(data_keys)
        file_path = os.path.join(self.tracker_dir, file_name)
        if file_name in os.listdir(self.tracker_dir):
            old_df = pd.read_csv(file_path, parse_dates=[
                'TIMESTAMP', 'UPLOAD_DATE', 'DOWNLOAD_DATE'
            ])
            tracker_df = __merge_trackers__(old_df, new_df)
        else:
            self.resources[resource]['tracker_file'] = file_name
            with open(self.resource_path, 'w') as wf:
                json.dump(self.resources, wf)
            tracker_df = new_df
        tracker_df.to_csv(file_path, index=False)
    
    def update_resource(self, resource, new=False):
        """
        Adding new resources or from NEM website to track, 
        or updating existing ones.
        """
        url = self.base_url + resource
        if resource in self.resources.keys() and new:
            print('Resource already tracked!')
            print('Set new to False to update.')
        else:                       
            self._resource_dict_entry(resource, url)
            self._resource_csv_tracker(resource)

    def bulk_update(self):
        """
        """
        for resource in self.resources.keys():
            self.update_resource(resource)
