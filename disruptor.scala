import com.lmax.disruptor.dsl.Disruptor
import java.util.concurrent.Executors
import com.lmax.disruptor._
import com.mongodb.casbah.Imports._
import voldemort.client._
import com.mongodb.casbah.WriteConcern
//import com.twitter.cassie._
//import com.twitter.cassie.codecs.Utf8Codec
//import com.twitter.cassie.types.LexicalUUID
// TODO: unfortunate
//import scala.collection.JavaConversions._
//import com.twitter.logging.Logger

object Main {
  val bootstrapUrl = "tcp://localhost:6666"
  val factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
  val client = factory.getStoreClient[String, String]("test");

  val mongoConn = MongoConnection()
  val coll = mongoConn("disruptor")("trade")
  WriteConcern.Safe
  
  //val cluster = new Cluster("localhost")
  //val keyspace = cluster.keyspace("Keyspace1").connect()
  //val cass = keyspace.columnFamily("Standard1", Utf8Codec, Utf8Codec, Utf8Codec)

  case class ValueEvent(var value: Long, var trade:String)

  case class ValueEventTranslator(value: Long,trade:String) extends EventTranslator[ValueEvent] {
    def translateTo(event: ValueEvent, sequence: Long) = {
     event.value = value
     event.trade = trade
     event
    }
  }

  class ValueEventHandler extends EventHandler[ValueEvent] {
    def onEvent(event: ValueEvent, sequence: Long, endOfBatch: Boolean) {
        //val builder = MongoDBObject.newBuilder
        //builder += "value" -> event.value
        //builder += "trade" -> event.trade
        client.put(event.value.toString,event.trade)

        //coll += builder.result.asDBObject
        //println(event.value + event.trade + " added trade to voldemort")
    
    }
  }

  class MongoValueEventHandler extends EventHandler[ValueEvent] {
    def onEvent(event: ValueEvent, sequence: Long, endOfBatch: Boolean) {
        val builder = MongoDBObject.newBuilder
        builder += "value" -> event.value
        builder += "trade" -> event.trade
        //client.put(event.value.toString,event.trade)

        coll += builder.result.asDBObject
        //println(event.value + event.trade + " added trade to mongo")
    
    }
  }

  //class CassValueEventHandler extends EventHandler[ValueEvent] {
  //  def onEvent(event: ValueEvent, sequence: Long, endOfBatch: Boolean) {
  //     cass.insert(event.value.toString, Column("trade", event.trade)).apply()
  //      println(event.value + event.trade + " added trade to cass")
  //  
  //  }
  //}



  def main(args: Array[String]) {
    val ring_size = args(0).toInt

    val executor = Executors.newFixedThreadPool(args(1).toInt)

    val factory = new EventFactory[ValueEvent] {
      def newInstance() = ValueEvent(0L, "")
    }

    val handler1 = new ValueEventHandler
    val handler2 = new MongoValueEventHandler
    //val handler3 = new CassValueEventHandler

    val disruptor = new Disruptor[ValueEvent](factory, executor, new SingleThreadedClaimStrategy(ring_size), 
                                              new SleepingWaitStrategy())

    disruptor.handleEventsWith(handler2)


    disruptor.start()
    time {
    //Publishing
      for (i <- 1 to 1000000) {
        disruptor.publishEvent(ValueEventTranslator(i, "Trade:"+i.toString))
        //println(coll.count)
      }
    }

    disruptor.shutdown()
    executor.shutdown()


    }

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: "+(System.nanoTime-s)/1e6+"ms")
    ret
  }


}