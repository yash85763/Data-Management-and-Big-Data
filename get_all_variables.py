import os
import pprint as pp

### Setting up environment variables
os.environ['envn'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

### Get the environment variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

### Setting up other variables
appName = "USA Prescriber Research Report"
current_path = os.getcwd()
staging_dim_city = current_path + '\..\staging\dimension_city'
staging_fact = current_path + '\..\staging\\fact'