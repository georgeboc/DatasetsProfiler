from setuptools import setup, find_packages

setup(
    name="DatasetsEvaluation",
    version="1.0",
    packages=find_packages(where="./src"),
    install_requires=[
        "prettytable>=2.0.0",
        "pyspark>=3.0.1",
        "dependency-injector>=4.19.0"
    ],
    package_dir={
        '': 'src',
    },
    py_modules = ['__main__']
    )
