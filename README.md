# go-stream

`go-stream` will allow stream reading/writing on `Storage` via adding a new stream layer.

This lib will maintain data between under and upper storage layer.

## Status

This project is a **Proof of Concept**, please connect with us via issues if you have interest on it.

## Usage

### Init a stream

Init a stream via upper and under storage.
All data will be written into upper first.
`go-stream` will persist data from upper to under async.

```go
s, err := stream.NewWithConfig(&stream.Config{
    Upper:         upperStore,
    Under:         underStore,
    // Set speed limit here to prevent upper been written too fast.
    SpeedLimit:    10 * 1024 * 1024,
    PersistMethod: stream.PersistMethodMultipart,
})
if err != nil {
	return err
}
go s.Serve()
go func() {
    for v := range s.Errors() {
        log.Printf("got error: %v", v)
    }
}()
```

### Start a new branch

```go
br, err := s.StartBranch(id, name)
if err != nil {
	return err
}
```

### Write data into a branch

- `Write`

Data should be written in order.

```go
n, err := br.Write(uint64(i), bs)
if err != nil {
    t.Fatal(err)
}
```

- `ReadFrom`

Data will be read from the Reader until `io.EOF`.

```go
n, err := br.ReadFrom(r)
if err != nil {
    t.Fatal(err)
}
```

### Complete a branch

Call `Complete` to finish a branch write job.
Only after this call, the written data is persisted.

```go
err = br.Complete()
if err != nil {
    t.Fatal(err)
}
```
