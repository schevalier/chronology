SHELL := /bin/bash

ifndef JIA_VERSION
$(error Please specify a JIA_VERSION, like `JIA_VERSION=v0.7.0 make build`)
endif

generate_dockerfile:
	mkdir -p $(JIA_VERSION)
	sed -e "s/__TAG__/$(JIA_VERSION)/" Dockerfile.template > $(JIA_VERSION)/Dockerfile
	cp supervisord.conf $(JIA_VERSION)
build:
	cd $(JIA_VERSION) && sudo docker build -t chronology/jia:$(JIA_VERSION) .
build_nocache:
	cd $(JIA_VERSION) && sudo docker build --no-cache -t chronology/jia:$(JIA_VERSION) .
run:
	sudo docker run -p 8152:8152 chronology/jia:$(JIA_VERSION)
run-terminal:
	sudo docker run -t -i chronology/jia:$(JIA_VERSION) /bin/bash
push:
	sudo docker push chronology/jia:$(JIA_VERSION)
ip:
	boot2docker ip
