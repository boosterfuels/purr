from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(name='purr',
      version='0.0.1',
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