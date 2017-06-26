## Benchmark
The benchmark is described in:
http://sydney.edu.au/engineering/it/research/tr/tr693.pdf
There is a read-only transaction and a read-update transaction.

## Files
main.go: benchmark file
postgresql.conf: the postgresql configuration file I used.

## Usage
go run main.go -nthreads <threads> -duration <test time> -read <read ratio(0-1.0)>

