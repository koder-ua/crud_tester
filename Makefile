.PHONY: clean rebuild

crud_tester: export GOPATH = /home/koder/bin/golib
crud_tester: export PATH = /home/koder/bin/go/bin:$PATH
crud_tester: *.go
		go build -o crud_tester crud_me.go crud_connections.go

clean:
		rm crud_tester

rebuild: clean crud_tester
