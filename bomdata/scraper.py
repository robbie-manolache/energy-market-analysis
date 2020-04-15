import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime as dt
import pandas as pd
from __common__ import user_choice

class BOM_scraper:
    """
    """

    def __init__(self, data_dir, 
                 base_url="http://www.bom.gov.au"):
        """
        """
        self.data_dir = data_dir       
        self.base_url = base_url
        self.station_id = None
        self.station_url = None
        self.station_dir = None
        self.download_links = None

    def _get_file_links(self, paths):
        """
        """
        r = requests.get(self.station_url)
        soup = BeautifulSoup(r.text)
        links = {}
        paths = "/climate/dwo/"
        for link in soup.findAll('a'):
            pattern = r'\D\D\D\s\d\d'
            link_name = bool(re.match(pattern, link.text))
            link_url = link.get('href').startswith(paths)
            if link_name and link_url:
                csv_link = link.get('href'
                              ).replace('/html','/text'
                              ).replace('.shtml','.csv')
                k = link.text.replace('\xa0','-')
                links[k] = self.base_url+csv_link
        return links

    def set_station_id(self, station_id):
        """
        """
        self.station_id = station_id 
        self.station_dir = os.path.join(self.data_dir, station_id) 
        if os.path.isdir(self.station_dir):
            pass
        else:
            os.makedirs(self.station_dir)     

    def scrape_station(self):
        """
        """
        paths = "/climate/dwo/"
        if self.station_id is not None:
            self.station_url = self.base_url + paths + \
                            self.station_id + ".latest.shtml"
        else:
            print("Set station ID first!")
            return
        self.download_links = self._get_file_links(paths)

    def _download_csv(self, url):
        """
        """
        r = requests.get(url)
        name = url.split('/')[-1].split('.')[-2] + '.csv'
        path = os.path.join(self.station_dir, name)
        with open(path, 'wb') as wf:
            wf.write(r.content)

    def download_files(self, dates='all'):
        """
        """
        if dates == 'all':
            dates = list(self.download_links.keys())
        else:
            pass
        for date, url in self.download_links.items():
            if date in dates:
                self._download_csv(url)

    def load_station_data(self):
        """
        """
        df_list = []
        for file in os.listdir(self.station_dir):
            path = os.path.join(self.station_dir, file)
            df_list.append(pd.read_csv(path, skiprows=9,
                                       encoding='ISO-8859-1'))
        return pd.concat(df_list, ignore_index=True)

    

