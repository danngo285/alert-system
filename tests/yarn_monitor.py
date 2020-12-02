import os
import sys
from datetime import datetime
from typing import List
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.spark_job import SparkJob
from alert.ms_teams import MsTeams_Card

class alert_job:
    def __init__(self, line):
        if line is not None and len(line.split(",")) > 0:
            args = line.split(",")
            self.name = args[0]
            self.interval_check = int(args[1]) if len(args) > 1 and args[1].isdigit() else None
            self.last_failed = int(args[2]) if len(args) > 2 and args[2].isdigit() else None
            self.last_notified = int(args[3]) if len(args) > 3 and args[3].isdigit() else None
        else:
            raise Exception ("InputError: cannot establish class job")

    def is_not_notified(self, app: SparkJob, is_running: bool):
        if is_running:
            last_finished = app.get_finished_timestamp(True)
            unix = int(datetime.strptime(last_finished, '%Y-%m-%d %H:%M:%S').timestamp())
            return True if unix >= self.last_failed + self.interval_check else False
                
        else:
            now = int(datetime.now().timestamp())
            return True if now >= self.last_notified + self.interval_check else False

    
    def update_last_failed(self, timestamp: str):
        self.last_failed = int(datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').timestamp())

    def update_last_notified(self, timestamp: str):
        self.last_notified = int(datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').timestamp())

    def get_last_notified(self):
        return self.last_notified

    def get_last_failed(self):
        return self.last_failed

    def get_interval_check(self):
        return self.interval_check

    def __str__(self):
        return "%s,%s,%s,%s" % (self.name, self.interval_check, self.last_failed, self.last_notified)

def get_monitor_jobs(path: str):
    with open(path, "r") as f:
        data = f.read().splitlines()
        app_name = [alert_job(line) for line in data]

    return app_name

def is_needed_noti(app: alert_job):
    spark_app = SparkJob(job_name = app.name)
    if not spark_app.is_running():
        if spark_app.get_last_failed() is not None and app.is_not_notified(spark_app.get_last_failed(), False):
            return spark_app
        else:
            return None

    elif spark_app.get_last_failed() is not None and app.is_not_notified(spark_app.get_last_failed(), True):
        return spark_app

    else:
        return None


if __name__ == '__main__':
    app_file = "%s/app_names" % (os.path.dirname(__file__))
    all_apps: List[alert_job] = get_monitor_jobs(app_file)
    need_noti_apps = [(app, is_needed_noti(app)) for app in all_apps if is_needed_noti(app) is not None]
    new_info = {app.name: str(app) for app in all_apps}
    
    for alert_app, spark_app in need_noti_apps:
        last_failed: SparkJob = spark_app.get_last_failed()
        teams = MsTeams_Card("TEST_MS_TEAMS")
        message = teams.build_spark_app_card(last_failed)
        teams.send_message(message)
        alert_app.update_last_failed(last_failed.get_finished_timestamp(True))
        alert_app.update_last_notified(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        new_info[alert_app.name] = str(alert_app)
                                                                                
    with open(app_file, "w") as f:
        f.write("\n".join(str(app) for app in new_info.values()))
