from setuptools import setup, find_packages

setup(
    name="DatasetsProfiler",
    version="1.3",
    python_requires='>=3.8.5',
    packages=find_packages(include=["datasets_profiler", "datasets_profiler.*"]),
    package_data={'': ['resources/*.conf']},
    install_requires=[
        "prettytable>=2.0.0",
        "pyspark>=3.0.1",
        "dependency-injector>=4.19.0",
        "numpy>=1.20.1",
        "avro>=1.10.2",
        "PyHDFS>=0.3.1"
    ],
    py_modules=['__main__']
    )
