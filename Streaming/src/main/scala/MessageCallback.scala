import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.log4j.Logger

object MessageCallback extends Callback {
  //private val Log = Logger.getLogger(MessageCallback.getClass)
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      //Log.error("Message dispatch was not successful", exception)
      println(exception.getMessage)
    } else {
      /*Log.info(
        "RecordMetadata: Topic: {} Offset: {} Partition: {}",
        metadata.topic,
        metadata.offset,
        metadata.partition
      )*/
      println("MESSAGES SENT SUCCESSFULLY")
    }
  }
}
