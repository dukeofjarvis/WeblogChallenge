from setuptools import setup

setup(name='analytics',
      version='0.2',
      description='analytics for web logs',
      author='Graham Greenland',
      packages=['analytics'],
      test_suite='nose.collector',
      tests_require=['nose'],
      install_requires=[
          'nose',
      ]
      )