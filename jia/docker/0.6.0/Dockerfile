# On OSX, container port forwarding sucks, so type `boot2docker ip`
# and use that IP to test Jia locally instead of localhost.

FROM ubuntu:14.04

RUN apt-get update
RUN apt-get install -y software-properties-common        
RUN apt-add-repository -y ppa:chris-lea/node.js
RUN apt-get update
RUN apt-get install -y git python2.7 python-setuptools python-dev libevent-dev \
    nodejs supervisor python-pandas
RUN git clone https://github.com/Locu/chronology.git chronology
WORKDIR chronology/jia
RUN git checkout de2001303d2e9c8c2287c9f8ead652e7516b10fc
RUN easy_install pip
RUN pip install -r requirements.txt
RUN npm install -g grunt-cli
RUN npm install
RUN grunt build
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
CMD ["/usr/bin/supervisord"]
