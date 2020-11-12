import os
import sys
import json
import requests
<<<<<<< HEAD
from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

class SparkJob:
    def __init__(self, job_id: str, job_name: str = None, started_time: str = None, finished_time: str = None, tracking_url: str = None, owner: str = None, **kwargs):   
        if kwargs is None:
=======
import pendulum
from datetime import datetime
from configparser import ConfigParser

config = ConfigParser()
config.read('config/config.ini')
local_tz = pendulum.timezone('Asia/Saigon')

class SparkJob:
    def __init__(self, job_id: str = None, job_name: str = None, started_time: str = None, finished_time: str = None, tracking_url: str = None, owner: str = None, **kwargs):   
        if not kwargs:
            if job_id is not None or job_name is not None:
                self.job_id = job_id if job_id is not None else None
                self.job_name = job_name if job_name is not None else None
                self.started_time = started_time
                self.finished_time = finished_time
                self.owner = owner
                self.tracking_url = tracking_url
            else:
                raise Exception('InputError: id or name must have one of not None')
        else:
            self.job_id = kwargs['id']
            self.job_name = kwargs['name']
            self.started_time = kwargs['startedTime']
            self.finished_time = kwargs['finishedTime']
            self.owner = kwargs['user']
            self.tracking_url = kwargs['trackingUrl']
	
    def __repr__(self):
        return """
		id: %s
		name: %s
		owner: %s
		started_time: %s
		finished_time: %s
		tracking_url: %s
		""" % (self.job_id, self.job_name, self.owner, self.get_started_timestamp(), self.get_finished_timestamp(), self.tracking_url)
        
    def is_running(self):
        rm_url = config['YARN']['rm_url']
        params = {
            'states':'RUNNING'
        }
        resp = requests.get(rm_url, params=params)
        data = json.loads(resp.content)
        apps = data['apps']['app']
        if data is not None:
            if self.job_id is None:
                apps = [app['name'] for app in apps]
                return self.job_name in apps
            else:
                apps = [app['id'] for app in apps]
                return self.job_id in apps
        return False
    
    def __get_instance(self, state: str = 'FINISHED', finalStatus: str = None):
        rm_url = config['YARN']['rm_url']
        body = {
            'name': self.job_name,
            'states': state,
            'finalStatus': finalStatus
        }
        resp = requests.get(rm_url, params=body) 
        data = json.loads(resp.content)
        app = [app for app in data['apps']['app'] \
                if (self.job_name is not None and self.job_name == app['name']) \
                or (self.job_id is not None and self.job_id == app['id'])]      
        
        if app:
            app.sort(key = lambda app: app['finishedTime'], reverse = True)
            return SparkJob(**app[0])
        else:
            return None

    def get_finished_timestamp(self, local_time = False):
        if self.finished_time is None:
            return None
        else:
            utc_time = datetime.utcfromtimestamp(self.finished_time / 1000)
            pdl_time = pendulum.instance(utc_time)
            tz_time = pdl_time.in_timezone(local_tz) if local_time else pdl_time
            return tz_time.to_datetime_string()
    
    def get_started_timestamp(self, local_time = False):
        if self.started_time is None:
            return None
        else:
            utc_time = datetime.utcfromtimestamp(self.started_time / 1000)
            pdl_time = pendulum.instance(utc_time)
            tz_time = pdl_time.in_timezone(local_tz) if local_time else pdl_time
            return tz_time.to_datetime_string()

    def get_last_failed(self):
        last_failed = self.__get_instance(finishStatus='FAILED')
        last_killed = self.__get_instance(state = 'KILLED', finishStatus='KILLED')
        if last_failed is None and last_killed is None:
            return None
        elif last_failed is None:
            return last_killed
        elif last_killed is None:
            return last_failed
        else
            return last_failed if last_failed.get_finished_timestamp() > last_killed.get_finished_timestamp() else last_killed
    
    def get_last_success(self):
        return self.__get_instance('SUCCESSED')
    
