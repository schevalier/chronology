BIN= node_modules/.bin
BROWSERIFY=$(BIN)/browserify
JSHINT=$(BIN)/jshint
MOCHA=$(BIN)/mocha

PHONY: kronosjs
clean:
	find . -type f -name '*~' -delete
	rm -rf kronos.js kronos.min.js
node_modules: package.json
	@npm install
kronosjs: clean node_modules
	@$(BROWSERIFY) index.js --standalone kronos --outfile kronos.js
	node minify.js
readme:
	python common/readmeify.py javascript < demo.js > README.md
test:
	 @$(MOCHA) --reporter list tests/*.js
demo:
	node demo.js
lint:
	@$(JSHINT) -c .jshintrc --exclude-path .jshintignore demo.js src/*.js tests/*js
