import papermill as pm
import os
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

params = dict()
paramsPath = ''
extraParams = dict(secretsPath='/home/ec2-user/COVID-19-data/secrets.json')
if paramsPath:
  with open('params.json', 'r') as paramsFile:
    params = json.loads(paramsFile.read())

isDone = False
def watch():
    global isDone
    while not isDone:
      sleep(15)
      os.system('echo "***Polling latest output status result***"')
      os.system('tail -n 15 /home/runner/work/COVID-19-data/COVID-19-data/papermill-nb-runner.out')
      os.system('echo "***End of polling latest output status result***"')

def run():
  global isDone
  try:
    pm.execute_notebook(
      input_path='/home/ec2-user/COVID-19-data/JH_COVID-19.ipynb',
      output_path='/home/ec2-user/COVID-19-data/output.ipynb',
      parameters=dict(extraParams, **params),
      log_output=True,
      report_mode=True
    )
  finally:
    isDone = True

results = []
with ThreadPoolExecutor() as executor:
  results.append(executor.submit(run))
  if False:
    results.append(executor.submit(watch))

for task in as_completed(results):
  try:
    task.result()
  except Exception as e:
    print(e, file=sys.stderr)
    sys.exit(1)
