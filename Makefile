.PHONY: clean rebuild

crud_tester: *.go
		go build -o crud_tester *.go

clean:
		rm crud_tester

rebuild: clean crud_tester
