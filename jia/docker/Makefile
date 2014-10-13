ifndef JIA_VERSION
	JIA_VERSION = 0.6.0
endif

build:
	cd $(JIA_VERSION) && docker build -t chronology/jia:$(JIA_VERSION) .
run:
	docker run -p 8152:8152 chronology/jia:$(JIA_VERSION)
run-terminal:
	docker run -t -i chronology/jia:$(JIA_VERSION) /bin/bash
push:
	docker push chronology/jia:$(JIA_VERSION)
ip:
	boot2docker ip
