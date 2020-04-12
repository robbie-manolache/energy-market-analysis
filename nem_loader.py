import os
import pandas as pd
from nem_extract import NEM_extractor

class NEM_loader(NEM_extractor):
    """
    """

    def __init__(self, data_dir, base_url="http://nemweb.com.au"):
        """
        """
        NEM_extractor.__init__(self, data_dir, base_url)
        self.available_file_df = None
        self.available_files = None
    
    def get_available_files(self):
        """
        """
        self.load_tracker_df()
        tracker_df = self.current_tracker_df
        df = tracker_df[tracker_df.DOWNLOADED]
        df.loc[:, 'FILE'] = df['URL'].apply(lambda x: 
                            x.split('/')[-1].replace('.zip','.CSV'))
        self.available_file_df = df[['TIMESTAMP', 'FILE']]
        self.set_time_range(df)
        self.available_files = os.listdir(self.resource_dir)
        if df.FILE.tolist() == self.available_files:
            pass
        else:
            print("WARNING: inconsistency detected in file records")


