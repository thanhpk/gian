# Gian
![Logo](logo.jpg?raw=true "Gian")

A file format that
* hard to kill *(on bit rot harddrive)*
* auto healing


### Usage
``` go
gian := NewGian("/tmp/myfile")
gian.Write([]byte("hello"))
gian.Write([]byte("goodbye"))
gian.ForceCommit()
```
