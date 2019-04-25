from setuptools import setup

setup(
    name='gtfs_tripify',
    packages=['gtfs_tripify'],    
    py_modules=['gtfs_tripify'],
    version='0.0.1',
    install_requires=['numpy', 'pandas', 'requests', 'gtfs-realtime-bindings'],
    extras_require={
        'tests': ['pytest']
    },
    description='TODO.',
    author='Aleksey Bilogur',
    author_email='aleksey.bilogur@gmail.com',
    url='https://github.com/ResidentMario/gtfs-tripify',
    download_url='https://github.com/ResidentMario/gtfs-tripify/tarball/0.0.1',
    keywords=['TODO'],
    classifiers=[]
)