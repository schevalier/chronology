.PHONY: lint test demo readme

lint:
	jshint -c .jshintrc --exclude-path .jshintignore  test/ *.js

test:
	 ./node_modules/.bin/mocha --reporter list

demo:
	node demo.js

readme:
	python common/readmeify.py javascript < demo.js > README.md
