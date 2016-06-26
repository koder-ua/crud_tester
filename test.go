package main

import "time"

func pm() {
	panic("XXX")
}

func main() {
	go pm();
	time.Sleep(100)
}