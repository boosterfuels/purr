import yaml
import monitor
import os

here = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(here, 'constants/keywords.yml')

def get_keywords():
  try:
    with open(path, 'r') as stream:
      try:
        file_keywords = yaml.load(stream)
        if not file_keywords:
          monitor.logging.error('Keyword file is empty.')
        # looking for reserved keywords 
        reserved = [item.split(",")[0] for item in file_keywords["pg"] if item.split(",")[1] == 'R']
        return reserved
      except yaml.YAMLError as exc:
        print(exc)
  except FileNotFoundError as err:
    monitor.logging.error('File not found.')

