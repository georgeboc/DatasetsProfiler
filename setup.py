from setuptools import setup, find_packages

setup(
    name="DatasetsEvaluation",
    version="1.0",
    packages=find_packages(where="./src"),
    install_requires=[
        "pyspark>=3.0.1"
    ],
    package_dir={
        '': 'src',
    },
    py_modules = ['__main__']
    )
