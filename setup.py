from setuptools import setup, find_packages

setup(
    name="vaccine-bias-remorse",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.20.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.9",
        ],
    },
    python_requires=">=3.8",
    author="Ashutosh Kumar",
    author_email="ak1825@rit.edu",
    description="Analysis of vaccine bias remorse in social media comments",
    keywords="vaccine, bias, analysis, nlp",
    url="https://github.com/ashu1069/DSCI789",
)