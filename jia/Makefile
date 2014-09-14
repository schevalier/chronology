PHONY: clean run
clean:
	find . -type f -name '*.py[cod]' -delete
	find . -type f -name '*~' -delete
run:
	python runserver.py
lint: clean
	flake8 .
installdeps:
	sudo pip install -r requirements.txt
