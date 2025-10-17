from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="data-processing-pipeline",
    version="1.0.0",
    author="Ramin Edjlal",
    author_email="ramin.edjlal1359@gmail.com",
    description="پایپلاین پردازش داده به زبان فارسی",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-username/Data-Processing-Pipeline",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.5.0",
        "numpy>=1.21.0",
    ],
)
