import os
import requests
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime as dt
import pandas as pd
from nemtel.tracker import NEM_tracker

class NEM_extractor(NEM_tracker):
    """
    """

    def __init__(self, data_dir, base_url="http://nemweb.com.au"):
        """
        """
        NEM_tracker.__init__(self, data_dir, base_url)
        self.resource_dir = None
        self.current_tracker_path = None
        self.current_tracker_df = None
        self.timestamp_min = None
        self.timestamp_max = None
        self.time_range = None
        self.download_df = None

    def _set_time_range(self, df):
        """
        """
        self.timestamp_min = df.TIMESTAMP.min()
        self.timestamp_max = df.TIMESTAMP.max()
        self.time_range = [self.timestamp_min, self.timestamp_max]

    def load_tracker_df(self):
        """
        """
        if self.selected_resource is None:
            self.select_resource()
        else:
            pass
        self.resource_dir = self.selected_resource['resource_dir']
        if os.path.isdir(self.resource_dir):
            pass
        else:
            os.makedirs(self.resource_dir)
        tracker_path = self.selected_resource['tracker_path']
        self.current_tracker_path = tracker_path
        tracker_df = pd.read_csv(tracker_path, parse_dates=[
            'TIMESTAMP', 'DOWNLOAD_DATE'
        ])
        self.current_tracker_df = tracker_df
        self._set_time_range(tracker_df)
        

    def adjust_time_range(self, date_min=None, date_max=None):
        """
        """
        for i, date in enumerate([date_min, date_max]):
            if date is not None:
                self.time_range[i] = pd.to_datetime(date)

    def set_download_df(self, latest=5, by_time_range=False, 
                        manual_df=None):
        """
        """
        df = self.current_tracker_df.copy()
        if by_time_range:
            df = df[(df['TIMESTAMP']>=self.time_range[0])&
                    (df['TIMESTAMP']<=self.time_range[1])&
                    (~df.DOWNLOADED)]
        elif manual_df is not None:
            df = manual_df
        else:
            df = df[~df.DOWNLOADED].sort_values(by='TIMESTAMP')
            df = df.tail(latest)
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
        # Archive typically contains zipped folders of zipped files
        # Hence need to check for zipped files in downloaded files 
          # and unzip any if found
        for f in os.listdir(self.resource_dir):
            if f.endswith('.zip'):
                path = os.path.join(self.resource_dir, f)
                with ZipFile(path, 'r') as zf:
                    zf.extractall(self.resource_dir)
                os.remove(path)
            else:
                pass
                               
