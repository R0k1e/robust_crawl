from setuptools import setup, find_packages

setup(
    name="robust_crawl",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "gevent",
        "playwright",
        "tenacity",
        "brotli",
        "numpy",
        "requests",
        "pyyaml",
        "bs4",
        "fake_useragent",
        "openai",
    ],
    author="Haoyu Wang",
    author_email="Haoyu_Wang_1103@outlook.com",
    description="A library for robust cralwer based on proxy pool and token bucket, support browser and requests",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/R0k1e/robust_crawl.git",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.11',
)