FROM python:3.12.7

WORKDIR /jobcreator

# Install job creator
COPY ./job_creator /jobcreator
RUN python -m pip install --no-cache-dir .

CMD ["jobcreator"]