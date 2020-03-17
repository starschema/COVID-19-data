import requests
import os
import subprocess
from datetime import datetime, timedelta

lastrunfile='/home/ec2-user/COVID-19-data/runs.txt'
ts_format="%Y-%m-%dT%H:%M:%SZ"

def format_ts(dt):
    return dt.strftime(ts_format)


def last_run():
    try:
        with open(lastrunfile) as f:
            for line in f:
                pass
            last_line = line
    except:
        return datetime.now() - timedelta(days=7)
    return datetime.strptime(last_line[:-1], ts_format) if last_line != None else datetime.now() - timedelta(days=7)

# Nothing fancy, just making sure we don't miss anything by a beat. Better do something twice than none at all
since = format_ts(last_run() - timedelta(minutes=1))

# Store to the filesystem when the current run started
with open(lastrunfile, mode='a') as file:
    file.write('%s\n' % 
               (format_ts(datetime.now())))


url = 'https://api.github.com/repos/CSSEGISandData/COVID-19/commits?since={}&path=csse_covid_19_data/csse_covid_19_time_series'.format(since)

response = requests.get(url)
commits =  response.json()

if len(commits) > 0:
    creds = ""
    with open('/home/ec2-user/COVID-19-data/secret2.json', 'r') as file:
        creds = file.read().replace('\n', '')
    os.environ["GSHEET_API_CREDENTIALS"] = creds
    print(os.environ["GSHEET_API_CREDENTIALS"])
    print("We should run now.")
    subprocess.run(['/usr/bin/python3', '/home/ec2-user/COVID-19-data/nb_runner.py'], env=os.environ)
else:
    print("Nothing to do now.")
