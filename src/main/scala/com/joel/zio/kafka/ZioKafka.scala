package com.joel.zio.kafka

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import zio.blocking.Blocking
import zio.{ZIO, ZLayer}
import zio.json._
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.stream.ZSink

object ZioKafka extends zio.App {

  // settings for the kafka consumer
  val consumerSettings = ConsumerSettings(List("joel-kafka:9092")).withGroupId("updates-consumer")

  // effectful resource
  val managedConsumer = Consumer.make(consumerSettings)

  // effectful dependency injection
  val consumer = ZLayer.fromManaged(managedConsumer)

  // streams of strings read from the kafka topic
  val footbalMatchesStream = Consumer.subscribeAnd(Subscription.topics("updates"))
    .plainStream(Serde.string, Serde.string)

  case class MatchPlayer(name: String, score: Int)  {
    override def toString = s"$name: $score"
  }
  object MatchPlayer {
    // gen relies on macros to generate code for the encoder
    implicit val encoder: JsonEncoder[MatchPlayer] = DeriveJsonEncoder.gen[MatchPlayer]
    implicit val decoder: JsonDecoder[MatchPlayer]  = DeriveJsonDecoder.gen[MatchPlayer]
  }
  case class Match(players: Array[MatchPlayer]){
    def score: String = s"${players(0)} - ${players(1)}"
  }
  object Match {
    // gen relies on macros to generate code for the encoder
    implicit val encoder: JsonEncoder[Match] = DeriveJsonEncoder.gen[Match]
    implicit val decoder: JsonDecoder[Match]  = DeriveJsonDecoder.gen[Match]
  }

  // json strings -> kafka -> jsons -> Match instances

  val matchSerde: Serde[Any, Match] = Serde.string.inmapM {
    string =>
      // deserialization
      ZIO.fromEither(string.fromJson[Match].left.map(errorMessage => new RuntimeException(errorMessage)))
  } {
    theMatch =>
      // serialization
      ZIO.effect(theMatch.toJson)
  }

  val matchesStream = Consumer.subscribeAnd(Subscription.topics("updates"))
    .plainStream(Serde.string, matchSerde)

  val matchesPrintableStream = matchesStream // stream of Match instances
    .map(cr => (cr.value.score, cr.offset)) // streams of tuples (String, offset)
    .tap { case (score, _) => zio.console.putStrLn(s"| $score |") }
    .map(_._2) // keep the offsets
    .aggregateAsync(Consumer.offsetBatches) // stream of offsets

  val streamEffect = matchesPrintableStream.run(ZSink.foreach(offset => offset.commit))

  override def run(args: List[String]) =
    streamEffect.provideSomeLayer(consumer ++ zio.console.Console.live).exitCode

}

object ZioKafkaProducer extends zio.App {

  import ZioKafka._
  val producerSettings = ProducerSettings(List("joel-kafka:9092"))
  val producerResource = Producer.make(producerSettings, Serde.string, matchSerde)
  val producer: ZLayer[Blocking, Throwable, Producer[Any, String, Match]] = ZLayer.fromManaged(producerResource)

  val finalScore = Match(Array(MatchPlayer("ITA",1), MatchPlayer("ENG",2)))

  val record = new ProducerRecord[String, Match]("updates", "update-3", finalScore)

  // creates a ZIO effect
  val producerEffect: ZIO[Producer[Any, String, Match], Throwable, RecordMetadata] = Producer.produce(record)

  override def run(args: List[String]) =
    producerEffect.provideSomeLayer(producer).exitCode
}
