from setuptools import setup, find_packages

setup(
    name='gcloud_helpers',
    version='0.1.0',
    description='Reusable Google Cloud helper functions for storage, secrets',
    author='Stanislav',
    author_email='bike4cats@gmail.com',
    packages=find_packages(),
    install_requires=[
        'google-cloud-storage',
        'google-cloud-secret-manager',
        'python-dotenv'
    ],
    python_requires='>=3.8',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
