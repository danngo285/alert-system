import os
import sys
import json
import requests
from configparser import ConfigParser

from models.spark_job import SparkJob

config = ConfigParser()
config.read('config/config.ini')

class MsTeams_Card:
    def __init__(self, auth: str):
        self.url = self.__get_url(auth)
        self.token = self.__get_token(auth)
        self.proxy = self.__get_proxy(auth)
        if self.url is None and self.token is None:
            raise Exception('Ms-teams establish failed: miss url & token')
        
    def __get_url(self, auth):
        return config[auth]['url']

    def __get_token(self, auth):
        return config[auth]['token']
        
    def __get_proxy(self, auth):
        return {'http': config[auth]['proxy_http'], 'https': config[auth]['proxy_https']}
        
    def build_spark_app_card(self, app: SparkJob):
        color = "CA2805"
        title = "SparkApp {}: Failed".format(app.job_name)
        cardjson = """
                {{
                    "@context": "https://schema.org/extensions",
                    "@type": "MessageCard",
                    "themeColor": "{color}",
                    "title": "{title}",
                    "summary": "Alert sent from monitoring minutely job",
                    "sections": [
                        {{
                            "startGroup": true,
                            "facts": [
                                {{
                                    "name": "Id:",
                                    "value": "{id}"
                                }},
                                {{
                                    "name": "Owner:",
                                    "value": "{owner}"
                                }},
                                {{
                                    "name": "Failed Time:",
                                    "value": "{failed_time}"
                                }}
                            ],

                        }},
                        {{
                            "startGroup": true
                        }}
                    ],
                    "potentialAction": [
                        {{
                            "@type": "OpenUri",
                            "name": "Tracking Url",
                            "targets": [
                                {{
                                    "os": "default",
                                    "uri": "{tracking_url}"
                                }}
                            ]
                        }}
                    ]
                }}
            """
        return cardjson.format(color=color, title=title, id=app.job_id, owner=app.owner, failed_time=app.get_finished_timestamp(local_time = True), tracking_url=app.tracking_url)
    
    def send_message(self, message):
        headers = {'Content-type': 'application/json'}
        try:
            resp = requests.post(self.url, data=message, proxies=self.proxy, headers=headers)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err) 
