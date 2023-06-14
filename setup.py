from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='pytebis',         # How you named your package folder (MyLib)
    packages=['pytebis'],   # Chose the same as "name"
    version='v0.5.02',      # Start with a small number and increase it with every change you make
    # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    license='mit',
    # Give a short description about your library
    description='Python Connector for TeBIS from Steinhaus',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='MrLight',                   # Type in your name
    author_email='mrlight1@gmx.de',      # Type in your E-Mail
    # Provide either the link to your github or to your website
    url='https://github.com/MrLight/pytebis',
    # I explain this later on
    #download_url='https://github.com/MrLight/pytebis/archive/v0.1.1-alpha.tar.gz',
    # Keywords that define your package best
    keywords=['Python', 'Connector', 'TeBIS', 'Steinhaus'],
    install_requires=[
        'numpy',
        'pandas',
        'cx_Oracle',
        'simplejson',
    ],
    classifiers=[
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Development Status :: 4 - Beta',
        # Define that your audience are developers
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',   # Again, pick a license
        # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
