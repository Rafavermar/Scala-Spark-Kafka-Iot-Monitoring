package models

import org.apache.spark.sql.{Encoder, Encoders}

import java.sql.Timestamp

object CustomEncoders {
  // TODO: Encoder for (String, Timestamp) tuples used ....
  implicit val stringTimestampEncoder: Encoder[(String, Timestamp)] = Encoders.tuple(Encoders.STRING, Encoders.TIMESTAMP)
  // TODO: Encoder for (String, String) tuples used ....
  implicit val stringStringEncoder: Encoder[(String, String)] = Encoders.tuple(Encoders.STRING, Encoders.STRING)
}
