from setuptools import setup

setup(name="pyarrow",
        version="0.1",
        packages=[],
        install_requires=[
            # 'pandas==1.4.2', # pandas is c-python, not supported by glue1.0
            'pyarrow==1.0.1']
    )
