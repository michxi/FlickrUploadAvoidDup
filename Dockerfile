FROM debian
RUN apt-get update && \
  apt-get install --yes --no-install-recommends python python-pip less vim && \
  pip install flickrapi && \
  rm -rf /var/cache/apt
COPY flickrUploadAvoidDup.py .