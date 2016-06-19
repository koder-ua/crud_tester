package main

import (
    "os"
    "fmt"
    "time"
    "sort"
    "sync"
    "flag"
    "errors"
    "strings"
    "io/ioutil"
    "sync/atomic"
    "gopkg.in/yaml.v2"
)

type StatResults struct {
    requests_send int
    requests_complete int
    lats []int
    error_msg string
}

type ConfigSwiftSettings struct {
    User string
    Password string
    Project string
    Urls []string
}

const (
    OP_GET = iota
    OP_DEL = iota
    OP_PUT = iota
)

type WorkerSettings struct {
    task_ch <- chan WorkChunk
    results_ch chan <- *StatResults
    ready_barrier *sync.WaitGroup
    crud_connector CRUDConnector
    timeout_ch <- chan interface{}
    realtime_io_counter_p *uint64
    realtime_err_counter_p *uint64
    op_type int
}

type ConfigS3Settings struct {
    Ak string
    Sk string
    Urls []string
}

type Settings struct {
    Op_count int
    Workers int
    Obj_size int
    Operation string
    Chunk_size int
    Obj_name_templ string
    Bucket string
    Time_limit int
    Warmup_time int
    Urls []string
    Auth string
}

type Results struct {
    Sett *Settings
    Req_count int
    Err_count int
    Test_time float32
    Iops float32
    Lat_percentiles [20]int
    Avg_lat int
}

const DEFAULT_CHUNK_PER_WORKER = 10

func time_nano() uint64 {
    return uint64(time.Now().UnixNano())
}

func worker_func(sett *WorkerSettings) {
    var stats StatResults
    stats.lats = make([]int, 0, 128)

    conn, err := sett.crud_connector.Connect()
    sett.ready_barrier.Done()

    if nil != err {
        stats.error_msg = err.Error()
        sett.results_ch <- &stats
        return
    }

    var op_func func(string, *WorkChunk) error

    for chunk := range sett.task_ch {
        switch chunk.op_type {
        case OP_GET:
            op_func = conn.Get
        case OP_PUT:
            op_func = conn.Put
        case OP_DEL:
            op_func = conn.Del
        default:
            panic("Unknown op")
        }
        for idx := chunk.start_counter; idx != chunk.stop_counter; idx++ {
            obj_name := fmt.Sprintf(chunk.name_template, idx)
            stats.requests_send++
            rstart_time := time_nano()

            err := op_func(obj_name, &chunk)

            curr_lat := (time_nano() - rstart_time) / 1000000
            stats.lats = append(stats.lats, int(curr_lat))

            if nil == err {
                stats.requests_complete++
                if nil != sett.realtime_io_counter_p {
                    atomic.AddUint64(sett.realtime_io_counter_p, 1)
                }
            } else {
                if nil != sett.realtime_err_counter_p {
                    atomic.AddUint64(sett.realtime_err_counter_p, 1)
                }
            }

            select {
            case <- sett.timeout_ch:
                break
            default:
            }
        }
    }

    sett.results_ch <- &stats
}

func run_test(sett *Settings, op_id int) (*Results, error) {
    if sett.Warmup_time != 0 {
        wup_sett := *sett
        wup_sett.Time_limit = sett.Warmup_time
        wup_sett.Warmup_time = 0
        _, err := run_test(&wup_sett, op_id)
        if err != nil {
            return nil, err
        }
    }

    task_ch := make(chan WorkChunk)
    results_ch := make(chan *StatResults)
    worker_timeout_ch := make(chan interface{})
    var rt_io_count, rt_err_count uint64

    ws := WorkerSettings{ready_barrier: &sync.WaitGroup{},
                         timeout_ch: worker_timeout_ch,
                         task_ch: task_ch,
                         results_ch: results_ch,
                         realtime_io_counter_p: &rt_io_count,
                         realtime_err_counter_p: &rt_err_count}

    ws.ready_barrier.Add(sett.Workers)

    auth_data := strings.Split(sett.Auth, " ")

    switch auth_data[0] {
    case "s3":
        if len(auth_data) != 3 {
            fmt.Printf("Broken auth data '%s'\n", sett.Auth)
            os.Exit(1)
        }
        ws.crud_connector = &S3Connector{auth_data[0],
                                         auth_data[1],
                                         sett.Urls[0],
                                         sett.Bucket}
    default:
        return nil, errors.New(fmt.Sprintf("'%s' storage i not supported\n", auth_data[0]))
    }

    for i := 0; i < sett.Workers; i++ {
        go worker_func(&ws)
    }

    ws.ready_barrier.Wait()
    start_time := time.Now().Unix()

    var stop_idx int    
    for start_idx := 0; start_idx < sett.Op_count; start_idx = stop_idx {
        stop_idx = start_idx + sett.Chunk_size 
        if stop_idx > sett.Op_count {
            stop_idx = sett.Op_count
        }
        task_ch <- WorkChunk{name_template: sett.Obj_name_templ,
                             start_counter: start_idx,
                             stop_counter: stop_idx,
                             param: 0,
                             op_type: op_id}
    }

    close(task_ch)

    var stats StatResults
    io_timed_data := make([]uint64, 0, 120)
    err_timed_data := make([]uint64, 0, 120)
    var test_timiout_ch <- chan time.Time

    if sett.Time_limit != 0 {
        test_timiout_ch = time.NewTimer(time.Second * time.Duration(sett.Time_limit)).C
    } else {
        test_timiout_ch = make(chan time.Time)
    }

    for i := 0; i < sett.Workers; {
        select {
        case w_stats := <- results_ch:
            stats.requests_send += w_stats.requests_send
            stats.requests_complete += w_stats.requests_complete
            stats.lats = append(stats.lats, w_stats.lats...)
            i++

        case <-time.After(time.Second):
            io_timed_data = append(io_timed_data,
                                   atomic.LoadUint64(&rt_io_count))
            err_timed_data = append(err_timed_data,
                                    atomic.LoadUint64(&rt_err_count))
        case <- test_timiout_ch:
            close(worker_timeout_ch)
        }
    }

    end_time := time.Now().Unix()
    test_time := float32(end_time - start_time)

    res := Results{Sett: sett,
                   Test_time: test_time,
                   Iops: float32(stats.requests_complete) / test_time,
                   Err_count: stats.requests_send - stats.requests_complete,
                   Req_count: stats.requests_send}

    for _, lat := range(stats.lats) {
        res.Avg_lat += lat
    }

    res.Avg_lat /= len(stats.lats)
    sort.Ints(stats.lats)

    for i := 0; i < 20 ; i++ {
        res.Lat_percentiles[i] = stats.lats[len(stats.lats) * (i + 1) / 20]
    }

    return &res, nil
}

func main() {
    var sett Settings
    to_yaml := flag.Bool("yaml", false, "output results in yaml format")
    flag.Parse()

    cfg_files := flag.Args()

    if len(cfg_files) != 1 {
        fmt.Printf("Usage %s config_file", os.Args[0])
        os.Exit(1)
    }

    data, err := ioutil.ReadFile(cfg_files[0])
    if err != nil {
        fmt.Printf("Faile to read file %s - %v", cfg_files[0], err)
        os.Exit(1)
    }

    err = yaml.Unmarshal(data, &sett)
    if err != nil {
        fmt.Printf("Can't parse config file %s - %v", cfg_files[0], err)
        os.Exit(1)
    }

    fmt.Printf("Settings %s\n", sett)
    os.Exit(0)

    if sett.Chunk_size == 0 {
        sett.Chunk_size = sett.Op_count / (sett.Workers * DEFAULT_CHUNK_PER_WORKER)
    }

    var op_id int
    switch sett.Operation {
    case "del":
        op_id = OP_DEL
    case "get":
        op_id = OP_GET
    case "put":
        op_id = OP_PUT
    default:
        fmt.Printf("Unknown operation '%s'. Only get/put/del allowed.\n", sett.Operation)
        os.Exit(1)
    }

    if len(sett.Urls) > 1 {
        fmt.Println("Multiple urls not supported yet")
        os.Exit(1)
    } else if len(sett.Urls) == 0 {
        fmt.Println("No connection urls provided!")
        os.Exit(1)
    }

    res, err := run_test(&sett, op_id)

    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }

    if !*to_yaml {
        fmt.Printf("Req. send %d\n", res.Req_count)
        fmt.Printf("Err count %d\n", res.Err_count)
        fmt.Printf("Avg RPS %f\n", int(res.Iops))
        fmt.Printf("Avg lat %d ms\n", res.Avg_lat)
        fmt.Printf("Med lat %d ms\n", res.Lat_percentiles[10])
        fmt.Printf("95perc lat %d ms\n", res.Lat_percentiles[19])
    } else {
        res, err := yaml.Marshal(res)
        if err != nil {
            fmt.Printf("Can't marshal data to yaml: %v\n", err)
            os.Exit(1)
        }
        fmt.Println(res)
    }
}
