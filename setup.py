from setuptools import setup, find_packages
from etl.monitor import logger

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
version = '0.1.7'
setup(name='purr',
      version=version,
      author='Anett Balázsics',
      email='anett.balazsics@digihey.com',
      url='https://github.com/boosterfuels/purr',
      packages=find_packages(),
      install_requires=requirements,
      entry_points={
          'console_scripts': [
              'purr = etl.__main__:main'
          ]
      }
      )

logger.info("Starting Purr v%s ..." % version)
