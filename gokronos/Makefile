.PHONY: readme test demo

demo:
	go run src/demo/demo.go

test:
	go test gokronos

readme:
	python common/readmeify.py golang < src/demo/demo.go > README.md
