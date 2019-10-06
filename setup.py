from setuptools import setup

setup(
    name='gtfs_tripify',
    packages=['gtfs_tripify'],    
    py_modules=['gtfs_tripify'],
    version='0.0.3',
    install_requires=['numpy', 'pandas', 'requests', 'gtfs-realtime-bindings', 'click'],
    extras_require={
        'develop': ['pytest']
    },
    entry_points='''
        [console_scripts]
        airscooter=airscooter.cli:cli
    ''',
    description='Turn GTFS-RT transit updates into historical arrival data.',
    author='Aleksey Bilogur',
    author_email='aleksey.bilogur@gmail.com',
    url='https://github.com/ResidentMario/gtfs-tripify',
    download_url='https://github.com/ResidentMario/gtfs-tripify/tarball/0.0.3',
    keywords=['TODO'],
    classifiers=[]
)
