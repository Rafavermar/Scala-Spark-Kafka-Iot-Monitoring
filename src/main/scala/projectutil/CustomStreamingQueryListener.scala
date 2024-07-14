package projectutil

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

class CustomStreamingQueryListener extends StreamingQueryListener {
  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    println(s"Query started: ${event.id}, name: ${event.name}")
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    println(s"Query made progress: ${event.progress}")
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    println(s"Query terminated: ${event.id}, with exception: ${event.exception.getOrElse("No exception")}")
  }
}
