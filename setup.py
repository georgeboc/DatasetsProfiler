from setuptools import setup, find_packages

setup(
    name="DatasetsEvaluation",
    version="1.2",
    python_requires='>=3.8.5',
    packages=find_packages(include=["datasets_evaluation", "datasets_evaluation.*"]),
    install_requires=[
        "prettytable>=2.0.0",
        "pyspark>=3.0.1",
        "dependency-injector>=4.19.0"
    ],
    py_modules=['__main__']
    )
