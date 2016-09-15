from setuptools import setup

setup(
    name='hmm_api',
    version='1.0',
    packages=['hmm_api', 'hmm_api.commands', 'hmm_api.etl', 'hmm_api.utils'],
    include_package_data=True,
    install_requires=[
        'click',
    ],
    entry_points='''
        [console_scripts]
        hmm_api=hmm_api.cli:cli
    ''',
)
