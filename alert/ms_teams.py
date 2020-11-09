import os
import sys
import json
import requests
import pendulum
from configparser import ConfigParser

from models.spark_job import SparkJob

config = ConfigParser()
config.read('config.ini')
local_tz = pendulum.timezone('Asia/Saigon')

class MsTeams_Card:
    def __init__(self, auth: str):
        self.url = self.__get_url(auth)
        self.token = self.__get_token(auth)
        self.proxy = self.__get_proxy(auth)
        
    def __get_url(self, auth):
        return {'http': config['TEAMS'][auth]['url']}

    def __get_token(self, auth):
        return {'http': config['TEAMS'][auth]['token']}
        
    def __get_proxy(self, auth):
        return {'http': config['TEAMS'][auth]['proxy']}
        
    def build_spark_app_card(self, app: SparkJob):
        failed_utc_time = app.finished_time
        failed_pdl_time = pendulum.instance(failed_utc_time)
        failed_tz_time = failed_pdl_time.in_timezone(local_tz).to_datetime_string()

        color = "CA2805"
        title = "App {}: Failed".format(app.job_name)

        cardjson = """
                {{
                    "@context": "https://schema.org/extensions",
                    "@type": "MessageCard",
                    "themeColor": "{color}",
                    "title": "{title}",
                    "summary": "{title}",
                    "sections": [
                        {{
                            "startGroup": true,
                            "facts": [
                                {{
                                    "name": "Owner:",
                                    "value": "`{owner}`"
                                }},
                                {{
                                    "name": "Failed Time:",
                                    "value": "`{failed_time}`"
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
        return cardjson.format(color=color, title=title, onwer=app.owner, failed_time=app.finished_time, tracking_url=app.tracking_url)
    
    def send_message(self, message):
        headers = {'Content-type': 'application/json'}
        if self.url is not None and self.token is not None:
            requests.Request('GET', self.__get_url, params=message, proxies=self.__get_proxy, headers=headers)
    
    