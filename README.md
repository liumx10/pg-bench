## Benchmark

### ssibench
The benchmark is described in:
http://sydney.edu.au/engineering/it/research/tr/tr693.pdf
There are three tables ssibench-{1,2,3} with two non-null integer and ten variable sized character columns b_value-{1,2,...,10}; one of integer column is a primary key b_int_key. Each table has 100K items. 

There is a read-only transaction and a read-update transaction.
Read-only transactions consists of a single select-from-where query:

    SELECT sum(b int value) FROM ssibench-i 
        WHERE b int key > :id and b int key <= :id+100
Read-update transactions reads 100 rows from ssibench-i and updates 20 rows from ssibench-((i%3)+1):
    
    SELECT sum(b int value) FROM ssibench-i 
        WHERE b int key > :id and b int key <= :id+100
    UPDATE ssibench-((i+1)%3)
        SET b value-k = :rand str
        WHERE b int key = :id1 
            OR b int key = :id2
            OR ... 
            OR b int key = :id20


### simple ssibench (default)

Abort rate of ssibench is very high (25% in my machine). So a transaction is easier to be aborted before its conflict list grows too long. 

I tranformed ssibench into a simpler one. There are a read-only transaction and a update-only transaction. Only one table is used so there are more conflicts. 

Read-only:

    SELECT sum(b int value) FROM ssibench-1
        WHERE b int key > :id and b int key <= :id+100

Update-only:

    UPDATE ssibench-1
        SET b value-k = :rand str
        WHERE b int key = :id1 
            OR b int key = :id2
            OR ... 
            OR b int key = :id20

### tpcb
It is the standard tpc benchmark.

## Usage

You have to install golang (>=1.6) first.
Then run 

    go run main.go --help

Tips: default means simple ssibench.

It relies on the package pq (https://github.com/lib/pq) 
If you have some problems when running the code, you can reinstall this package.