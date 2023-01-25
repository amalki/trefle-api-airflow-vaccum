from setuptools import setup, find_packages

setup(
    name="airflow",
    version="1.0",
    packages=find_packages(),
    python_requires=">=3.9",
    test_requires=["pytest"],
)
