import os
import requests
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime as dt
import pandas as pd
from nem_tracker import NEM_tracker

def __gen_tracker_dict__(res_dict):
    """
    """
    tracker_dict = {}
    for k in res_dict.keys():
        tracker_dict[k] = res_dict[k]['tracker_file']
    return tracker_dict


class NEM_extractor(NEM_tracker):
    """
    """

    def __init__(self, data_dir, base_url="http://nemweb.com.au"):
        """
        """
        NEM_tracker.__init__(self, data_dir, base_url)
        self.tracker_dict = __gen_tracker_dict__(self.resources)
        self.selected_resource = None
        self.resource_dir = None
        self.current_tracker_path = None
        self.current_tracker_df = None
        self.timestamp_min = None
        self.timestamp_max = None
        self.time_range = None
        self.download_df = None

    def select_resource(self):
        """
        """
        res_list = dict(enumerate(self.tracker_dict.keys()))
        choices = '\n'.join(['[%d] %s'%(k,v) for k,v in res_list.items()])
        idx = input("Select from: %s"%choices)
        self.selected_resource = res_list[int(idx)]

    def load_tracker_df(self, resource=None):
        """
        """
        if resource is None:
            if self.selected_resource is None:
                print("Must specify a resource!")
            else:
                resource = self.selected_resource
        else:
            pass
        file_name = self.tracker_dict[resource]
        self.resource_dir = os.path.join(self.data_dir, file_name[:-4])
        if os.path.isdir(self.resource_dir):
            pass
        else:
            os.makedirs(self.resource_dir)
        file_path = os.path.join(self.tracker_dir, file_name)
        self.current_tracker_path = file_path
        self.current_tracker_df = pd.read_csv(file_path, parse_dates=[
            'TIMESTAMP', 'UPLOAD_DATE', 'DOWNLOAD_DATE'
        ])
        self.timestamp_min = self.current_tracker_df.TIMESTAMP.min()
        self.timestamp_max = self.current_tracker_df.TIMESTAMP.max()
        self.time_range = [self.timestamp_min, self.timestamp_max]

    def adjust_time_range(self, date_min=None, date_max=None):
        """
        """
        for i, date in enumerate([date_min, date_max]):
            if date is not None:
                self.time_range[i] = pd.to_datetime(date)

    def set_download_df(self):
        """
        """
        df = self.current_tracker_df.copy()
        df = df[(df['TIMESTAMP']>=self.time_range[0])&
                (df['TIMESTAMP']<=self.time_range[1])&
                (~df.DOWNLOADED)]
        self.download_df = df

    def _download(self, url):
        """
        """
        r = requests.get(url)
        zf = ZipFile(BytesIO(r.content))
        zf.extractall(self.resource_dir)

    def download_files(self):
        """
        """
        for idx, row in self.download_df.iterrows():
            if self.current_tracker_df.loc[idx, 'DOWNLOADED']:
                pass
            else:
                self._download(row['URL'])
                now = pd.to_datetime(dt.now(
                       ).strftime('%Y-%m-%d-%H:%M:%S'))
                self.current_tracker_df.loc[
                    idx, ['DOWNLOADED', 'DOWNLOAD_DATE']
                ] = (True, now)
        self.current_tracker_df.to_csv(self.current_tracker_path, 
                                       index=False)
