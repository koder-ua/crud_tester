package main

import (
    "github.com/mitchellh/goamz/s3"
    "github.com/mitchellh/goamz/aws"
    "github.com/ton-katsu/goswift"
)

type WorkChunk struct {
    name_template string
    start_counter, stop_counter int
    param int
    op_type int
    content []byte
}

// ------------------------  INTERFACES ------------------------------------
type Storage interface {
    Del(string, *WorkChunk) error
    Get(string, *WorkChunk) error
    Put(string, *WorkChunk) error
}

type Connector interface {
    Connect() (Storage, error)
}

// ---------------------------- S3 -----------------------------------------

type S3Connector struct {
    ak string
    sk string
    url string
    bucket_name string
}

func (self *S3Connector) Connect() (Storage, error) {
    reg := aws.Region{Name: "cm-reg", S3Endpoint: self.url}
    s3_conn := s3.New(aws.Auth{self.ak, self.sk, ""}, reg)
    return &S3Connection{s3_conn.Bucket(self.bucket_name)}, nil
}

type S3Connection struct {
    bucket *s3.Bucket
}

func (conn *S3Connection) Del(obj_name string, chunk *WorkChunk) error {
    return conn.bucket.Del(obj_name)
}

func (conn *S3Connection) Get(obj_name string, chunk *WorkChunk) error {
    _, err := conn.bucket.Get(obj_name)
    return err
}

func (conn *S3Connection) Put(obj_name string, chunk *WorkChunk) error {
    err := conn.bucket.Put(obj_name, chunk.content,
                           "binary/octet-stream", s3.PublicReadWrite)
    return err
}

// ---------------------------SWIFT ----------------------------------------

type SwiftConnector struct {
    user string
    auth_url string
    password string
    url string
    tenant string
    region string
    bucket_name string
}

type SwiftConnection struct {
    client *goswift.Client
    bucket string
}

func (conn *SwiftConnection) Del(obj_name string, chunk *WorkChunk) error {
    return conn.client.DeleteObject(conn.bucket, obj_name)
}

func (conn *SwiftConnection) Get(obj_name string, chunk *WorkChunk) error {
    _, err := conn.client.GetObject(conn.bucket, obj_name)
    return err
}

func (conn *SwiftConnection) Put(obj_name string, chunk *WorkChunk) error {
    panic("Not implemented")
}

func (self *SwiftConnector) Connect() (Storage, error) {
    if self.auth_url != self.url {
        panic("Keystone auth not supported")
    }

    client := goswift.Client{AuthUrl: self.auth_url,
                             AccountName: self.user,
                             StorageUrl: self.url,
                             Password: self.password,
                             TenantName: self.tenant,
                             RegionName: self.region}
    err := client.SWAuthV1()
    if err != nil {
        return nil, err
    }

    return &SwiftConnection{&client, self.bucket_name}, nil
}
