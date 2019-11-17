import setuptools

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name="parapply", 
    version="0.0.1",
    author="Tan Nian Wei",
    author_email="tannianwei@aggienetwork.com",
    description="A simple drop-in replacement for parallelized pandas `apply`",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tnwei/parapply",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
)