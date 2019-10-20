from setuptools import setup, find_packages
from os import path
this_directory = path.abspath(path.dirname(__file__))

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

version = '1.0.6'

setup(name='purrito',
      version=version,
      author='Anett Balázsics',
      email='balazsicsanett@gmail.com',
      description="ETL tool for transfering data from MongoDB to PostgreSQL",
      url='https://boosterfuels.github.io/purr',
      packages=find_packages(),
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: MIT License",
          "Operating System :: OS Independent",
          "Topic :: Database"
      ],
      install_requires=requirements,
      entry_points={
          'console_scripts': [
              'purrito = etl.__main__:main'
          ]
      },
      long_description=long_description,
      long_description_content_type='text/markdown'
      )
