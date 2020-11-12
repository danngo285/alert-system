import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.spark_job import SparkJob
from alert.ms_teams import MsTeams_Card

def get_monitor_jobs(path: str):
    f = open(path, 'r')
    data = f.readlines()
    app_name = [app.split(',')[0] for app in data if app is not None]
    
    return app_name
    
def get_failed_apps():
    """[Summary]: This function will get all of apps has failed at the current time"

    Returns:
        List[SparkJob]: List failed spark apps
    """    
    all_apps: List[str] = get_monitor_jobs('%s/app_names' % (os.path.dirname(__file__)))
    failed_apps = []
    for app in all_apps:
        spark_app = SparkJob(job_name = app)
        if not spark_app.is_running():
	        failed_apps.append(spark_app)	
    return failed_apps
         
if __name__ == '__main__':
    failed_apps = get_failed_apps()
    for app in failed_apps:
        last_failed = app.get_last_failed()
        print(last_failed)
        teams = MsTeams_Card('TEST_MS_TEAMS')
        message = teams.build_spark_app_card(last_failed)
        teams.send_message(message)
    
