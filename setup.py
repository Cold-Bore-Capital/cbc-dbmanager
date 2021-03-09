from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'A python package to assist with connecting to Redshift via an SSH tunnel.'
LONG_DESCRIPTION = 'A python package to assist with connecting to Redshift via an SSH tunnel.'

# Setting up
setup(
    # Name of the folder containing module.
    name="cbcdbmanager",
    version=VERSION,
    author="CBC IT Group",
    author_email="it-group@coldborecapital.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    url='https://github.com/Cold-Bore-Capital/cbc-dbmanager',
    install_requires=[
        'psycopg2-binary>=2.8',
        'sshtunnel>=0.4.0',
        'pandas>=1.2'
    ],

    keywords=['python', 'redshift', 'ssh'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English"
    ]
)
