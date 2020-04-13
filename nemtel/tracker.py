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

def __gen_file_name__(data_keys, resource_type):
    """
    """
    if resource_type == 'Archive':
        i = -1
    else:
        i = -2
    name = set(map(lambda x: '-'.join(x.split('_')[:i]).lower(),
                   data_keys))
    name = list(name)   
    if len(name) > 1:
        print("More than one file type detected!")
        return None
    else:
        if resource_type == 'Archive':
            name = '-'.join([name[0],resource_type.lower()])
        else:
            name = name[0]
        return name + '.csv'

def __gen_tracker_df__(data_files, data_keys, resource_type):
    """
    """
    if resource_type == 'Archive':
        timestamp = pd.to_datetime(list(map(lambda x: 
                                   x.split('_')[-1][:-4], 
                                   data_keys)))
        version = resource_type                          
    else:
        timestamp = pd.to_datetime(list(map(lambda x: 
                                    x.split('_')[-2], 
                                    data_keys)))
        version = list(map(lambda x: 'V'+x.split('_')[-1][:-4], 
                                      data_keys))
    data_dict = {
        'TIMESTAMP': timestamp,
        'VERSION': version,
        'DOWNLOADED': False, 
        'DOWNLOAD_DATE': pd.to_datetime('19000101'),                          
        'URL': list(data_files.values())
    }
    return pd.DataFrame(data_dict)

def __merge_trackers__(old_df, new_df):
    """
    Add anything from new_df that is not in old_df already
    """
    merge_cols = ['TIMESTAMP', 'VERSION']
    old_temp = old_df.copy()[merge_cols]
    new_temp = new_df.copy()[merge_cols]
    merge_df = pd.merge(old_temp, new_temp, on=merge_cols,
                        how='outer', indicator=True)
    # rows no longer in CURRENT NEM reports
    old_rows = merge_df[merge_df["_merge"]=="left_only"]
    old_rows = old_rows[merge_cols].merge(old_df, on=merge_cols)
      # keep only old rows that have been downloaded
      # else they need to extracted from ARCHIVE or older records
    old_rows = old_rows[old_rows.DOWNLOADED]
    # for common rows, must keep old_df records
    com_rows = merge_df[merge_df["_merge"]=="both"]
    com_rows = com_rows[merge_cols].merge(old_df, on=merge_cols)
    # for new rows, use latest records
    new_rows = merge_df[merge_df["_merge"]=="right_only"]
    new_rows = new_rows[merge_cols].merge(new_df, on=merge_cols)
    return pd.concat([old_rows, com_rows, new_rows], 
                     ignore_index=True)

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

    def resources_report(self):
        """
        """
        for k, v in self.resources.items():
            print('\n%s\nLast update: %s'%(k, v['last_update']))

    def update_resource(self, resource):
        """
        """
        url = self.base_url + resource
        data_files = __get_links__(url, self.base_url)
        data_keys = list(data_files.keys())
        resource_type = resource.split('/')[2]
        file_name = __gen_file_name__(data_keys, resource_type)        
        self.resources[resource] = {
            'url': url,
            'type': resource_type,
            'tracker_file': file_name,
            'last_update': dt.now().strftime('%Y-%m-%d-%H:%M:%S')
        }
        with open(self.resource_path, 'w') as wf:
            json.dump(self.resources, wf)
        new_df = __gen_tracker_df__(data_files, data_keys, 
                                    resource_type)       
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
        For adding new resources.
        Resources must be specified as the relative URL
        path from the domain to the directory where all
        relevant data files are stored.
        E.g. /Reports/Current/Next_Day_Intermittent_DS/ 
        """
        if type(resources) is not list:
            resources = [resources]
        else:
            pass
        for resource in resources:
            self.update_resource(resource)

    def bulk_update(self):
        """
        For updating existing resources.
        """
        for resource in self.resources.keys():
            self.update_resource(resource)

    def select_resource(self, resource=None):
        """
        Select a resource for further processing.
        """
        d = {}
        if resource is None:
            res_list = self.resources.keys()
            name = user_choice(res_list)
        else:
            name = resource
        d['name'] = name
        tracker_file = self.resources[name]['tracker_file']
        d['tracker_path'] = os.path.join(self.tracker_dir, 
                                         tracker_file)
        d['resource_dir'] = os.path.join(self.data_dir, 
                                         tracker_file[:-4])         
        self.selected_resource = d                                                   
