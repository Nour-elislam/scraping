#Deriving the latest base image
FROM python:latest


#Labels as key value pair
LABEL Maintainer="nour"


# Any working directory can be chosen as per choice like '/' or '/home' etc
# i have chosen /usr/app/src
WORKDIR /usr/app/src

#to COPY the remote file at working directory in container
COPY scraping ./




CMD ['pip install git']
CMD ['git clone', 'https://github.com/Nour-elislam/scraping.git']
CMD['cd','scraping']
CMD['','. .venv/bin/activate']
CMD['cd','scripts']
CMD ['pip install e .']
CMD [ "python", "./test.py"]