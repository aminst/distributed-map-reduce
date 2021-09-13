# Distributed Map Reduce
I implemented this project as my self-learning effort in order to learn distributed system concepts.
It was part of the 6.824 MIT course lab projects, which are publicly available.  
* Course videos: https://www.youtube.com/channel/UC_7WrbZTCODu1o_kfUMq88g
* Course Resources: https://pdos.csail.mit.edu/6.824/schedule.html
## Implementation
I implemented the code similar to the original Google map-reduce paper.
Coordinator and worker processes are implemented separately and use RPCs to talk to each other.
Other features such as worker crash support are also implemented.
## Usage
### Coordinator
```
go run -race mrcoordinator.go <input_files...>
```
### Worker
```
go run -race mrworker.go <any>.so
```
### MapReduce plugins
```
go build -race -buildmode=plugin mrapps/<any>.go
```
