from setuptools import setup

setup(name='geoInfo',
      packages=['geoInfo'],
      version='0.1.1',
      description='Modules to manage geo info',
      author='Anant Shah',
      author_email='akshah@rams.colostate.edu',
      install_requires=[
          'pygeoip',
      ],
)