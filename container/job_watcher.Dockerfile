FROM python:3.12.3

WORKDIR /jobwatcher

# Install job watcher
COPY ./job_watcher /jobwatcher
RUN python -m pip install --no-cache-dir .

CMD ["jobwatcher"]