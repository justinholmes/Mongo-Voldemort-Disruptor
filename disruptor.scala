import com.lmax.disruptor.dsl.Disruptor
import java.util.concurrent.Executors
import com.lmax.disruptor._
import com.mongodb.casbah.Imports._
import voldemort.client._

object Main {
  val bootstrapUrl = "tcp://localhost:6666"
  val factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
  val client = factory.getStoreClient[String, String]("test");

  val mongoConn = MongoConnection()
  val coll = mongoConn("disruptor")("trade")


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
        println(event.value + event.trade + " added trade to voldemort")
    
    }
  }

  class MongoValueEventHandler extends EventHandler[ValueEvent] {
    def onEvent(event: ValueEvent, sequence: Long, endOfBatch: Boolean) {
        val builder = MongoDBObject.newBuilder
        builder += "value" -> event.value
        builder += "trade" -> event.trade
        //client.put(event.value.toString,event.trade)

        coll += builder.result.asDBObject
        println(event.value + event.trade + " added trade to mongo")
    
    }
  }



  def main(args: Array[String]) {
    val ring_size = args(0).toInt

    val executor = Executors.newFixedThreadPool(args(1).toInt)

    val factory = new EventFactory[ValueEvent] {
      def newInstance() = ValueEvent(0L, "")
    }

    val handler1 = new ValueEventHandler
    val handler2 = new MongoValueEventHandler


    val disruptor = new Disruptor[ValueEvent](factory, executor, new SingleThreadedClaimStrategy(ring_size), 
                                              new SleepingWaitStrategy())

    disruptor.handleEventsWith(handler1, handler2)


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