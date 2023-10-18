# Pull python package
FROM python:3.7

# Switch to root user
USER root

# Move file to work directory
ADD /scripts /usr/local/source

# Change Work Directory 
WORKDIR /usr/local/source

# Install pip requirements
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
# Change environment variable to remove __pycache__
RUN export PYTHONDONTWRITEBYTECODE=abc

# Run main file
CMD python main.py