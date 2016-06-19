package main

import (
    "github.com/mitchellh/goamz/s3"
    "github.com/mitchellh/goamz/aws"
)

type WorkChunk struct {
    name_template string
    start_counter, stop_counter int
    param int
    op_type int
}

// ------------------------  INTERFACES ------------------------------------
type CRUDStorage interface {
    Del(string, *WorkChunk) error
    Get(string, *WorkChunk) error
    Put(string, *WorkChunk) error
}

type CRUDConnector interface {
    Connect() (CRUDStorage, error)
}

// ---------------------------- S3 -----------------------------------------

type S3Connector struct {
    ak string
    sk string
    url string
    bucket_name string
}

func (self *S3Connector) Connect() (CRUDStorage, error) {
    reg := aws.Region{Name: "cm-reg", S3Endpoint: self.url}
    s3_conn := s3.New(aws.Auth{self.ak, self.sk, ""}, reg)
    return &S3CRUDConnection{s3_conn.Bucket(self.bucket_name)}, nil
}

type S3CRUDConnection struct {
    bucket *s3.Bucket
}

func (conn *S3CRUDConnection) Del(obj_name string, chunk *WorkChunk) error {
    return conn.bucket.Del(obj_name)
}

func (conn *S3CRUDConnection) Get(obj_name string, chunk *WorkChunk) error {
    _, err := conn.bucket.Get(obj_name)
    return err
}

func (conn *S3CRUDConnection) Put(obj_name string, chunk *WorkChunk) error {
    panic("Not implemented")
}

// ---------------------------SWIFT ----------------------------------------

type SwiftConnector struct {
    ak string
    sk string
    url string
    bucket_name string
}

func (self *SwiftConnector) connect() CRUDStorage {
    panic("Not implemented")
    return nil
}
