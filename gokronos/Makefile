.PHONY: readme test demo install

demo:
	go run src/demo/demo.go

test: installdeps
	go test gokronos

installdeps:
	go get github.com/stretchr/testify/assert

readme:
	python common/readmeify.py golang < src/demo/demo.go > README.md
