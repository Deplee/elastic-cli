from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="elastic-cli",
    version="1.0.0",
    author="izuna",
    author_email="dkapitsev@gmail.com",
    description="Интерактивный CLI-инструмент для управления Elasticsearch кластерами",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Deplee/elastic-cli",
    py_modules=["elastic_cli"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: System :: Systems Administration",
        "Topic :: Utilities",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "elastic-cli=elastic_cli:main",
        ],
    },
    keywords="elasticsearch, cli, devops, monitoring, management",
    project_urls={
        "Bug Reports": "https://github.com/Deplee/elastic-cli/issues",
        "Source": "https://github.com/Deplee/elastic-cli",
        "Documentation": "https://github.com/Deplee/elastic-cli#readme",
    },
)
