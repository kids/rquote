from os import path as os_path
from setuptools import setup

import rquote

this_directory = os_path.abspath(os_path.dirname(__file__))

def read_file(filename):
    with open(os_path.join(this_directory, filename), encoding='utf-8') as f:
        long_description = f.read()
    return long_description

def read_requirements(filename):
    return [line.strip() for line in read_file(filename).splitlines()
            if not line.startswith('#')]

setup(
    name='rquote',
    python_requires='>=3.4.0',
    version='0.1.8',
    description='Mostly day quotes of cn/hk/us/fund/future markets, side with quote list fetch',
    long_description=read_file('README.md'),
    long_description_content_type="text/markdown",
    author="Roizhao",
    author_email='roizhao@gmail.com',
    url='https://github.com/kids/rquote',
    packages=[
        'rquote'
    ],
    install_requires=read_requirements('requirements.txt'),
    include_package_data=True,
    license="MIT",
    keywords=['quotes', 'stock', 'rquote'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
