#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
      name='as207960-python-utils',
      version='1.2',
      author='Q 🦄',
      author_email='q@magicalcodewit.ch',
      install_requires=[
            'django-keycloak-auth @ git+https://github.com/TheEnbyperor/django-keycloak-auth#egg=django-keycloak-auth'
      ],
      packages=find_packages(),
)

