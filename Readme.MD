# ZIO Kafka Example

https://blog.rockthejvm.com/zio-kafka/

https://youtu.be/ExFjjczwwHs

`docker exec -it broker bash`

`kafka-console-producer --topic updates --broker-list localhost:9092 --property parse.key=true --property key.seperator=,`

`update-1,{"players":[{"name":"ITA","score":0},{"name":"ENG","score":1}]}`

## ZIO

ZIO is a library for asynchronous and concurrent programming that is based on pure functional
programming: https://zio.dev/docs/overview/overview_index. At the core there are ZIO effects which are
monads encapsulating IO operations.

A ZLayer describes a layer of the application (https://zio.dev/docs/datatypes/contextual/zlayer).

## Monads

To repeat things, first we have to understand again a monad. A monad is a pattern to chain opterations (
cmp. https://stackoverflow.com/questions/44965/what-is-a-monad). A chain of operations expressed a traditional way looks
like this

`[1,2,3].map(a => a + 1).filter(b => b != 3)`

while the same as monad looks like this

`[1,2,3].flatMap(a => [a + 1]).flatMap(b => b != 3 ? [b] : [])`

