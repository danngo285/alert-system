import os
import sys
import json
import requests
from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

class SparkJob:
    def __init__(self, job_id: str, job_name: str = None, started_time: str = None, finished_time: str = None, url_tracking: str = None, owner: str = None, **kwargs):   
        if kwargs is None:
            if job_id is not None or job_name is not None:
                self.job_id = job_id if job_id is not None else None
                self.job_name = job_name if job_name is not None else None
                self.started_time = started_time
                self.finished_time = finished_time
                self.owner = owner
            else:
                raise Exception('InputError: id or name must have one of not None')
        else:
            self.job_id =kwargs['id']
            self.job_name =kwargs['name']
            self.started_time =kwargs['startedTime']
            self.finished_time =kwargs['finishedTime']
            self.owner =kwargs['user']
        
        
    def is_running(self):
        rm_url = config['YARN']['rml_url']
        body = {
            'states':'RUNNING'
        }
        resp = requests.get(rm_url, params=body)
        data = json.loads(resp.text())
        if data is not None:
            if self.job_id is None:
                apps = [app['name'] for app in data['apps']]
                return self.job_id in apps
            else:
                apps = [app['id'] for app in data['apps']]
                return self.job_name in apps
        return False
    
    def __get_instance(self, state: str = None):
        rm_url = config['YARN']['rml_url']
        body = {
            'name': self.job_name,
            'states':'FINISHED',
            'finalStatus': state,
            'limit': 1
        }
        resp = requests.get(rm_url, params=body)
        app = resp['apps']['app']
        
        if app is not None:
            return SparkJob(app)
        else:
            return app
        
    def get_last_failed(self):
        return self.__get_instance('FAILED')
    
    def get_last_success(self):
        return self.__get_instance('SUCCESSED')
    