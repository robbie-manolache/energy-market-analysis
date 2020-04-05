import os
import requests
from io import BytesIO
from zipfile import ZipFile
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
        self.current_tracker_df = None
        self.timestamp_min = None
        self.timestamp_max = None
        self.time_range = None

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
        file_path = os.path.join(self.tracker_dir, file_name)
        self.current_tracker_df = pd.read_csv(file_path)
        self.timestamp_min = self.current_tracker_df.TIMESTAMP.min()
        self.timestamp_max = self.current_tracker_df.TIMESTAMP.max()
        self.time_range = [self.timestamp_min, self.timestamp_max]

    def adjust_time_range(self, date_min=None, date_max=None):
        """
        """
        for i, date in enumerate(date_min, date_max):
            if date is not None:
                self.time_range[i] = pd.to_datetime(date)