.PHONY: fmt
fmt:
	gofmt -w -s ./
	clang-format -style=google -i *.h *.c