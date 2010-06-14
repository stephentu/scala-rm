/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote


import java.lang.ClassNotFoundException

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, EOFException, IOException}

abstract class Serializer {

  var service: Service = _   

  /**
   * Given a message, optionally return any metadata about the message
   * serialized into a byte array
   */
  def serializeMetaData(message: AnyRef): Option[Array[Byte]]

  /** 
   * Given a message, return a byte array representation of the message
   * without any of its associated metadata
   */  
  def serialize(message: AnyRef): Array[Byte]

  /**
   * Given an optional metadata byte array and a data byte array, 
   * deserialize the data byte array into an object
   */
  def deserialize(metaData: Option[Array[Byte]], data: Array[Byte]): AnyRef

  /** 
   * Helper method to serialize the class name of a message
   */
  def serializeClassName(o: AnyRef): Array[Byte] = o.getClass.getName.getBytes

  /** Unique identifier used for this serializer */
  def uniqueId: Long

  /** Expects data to be in format [ int, bytes ]. Returns null if the length
   * read is 0. */
  @throws(classOf[IOException])
  private def readBytes(inputStream: DataInputStream): Array[Byte] = {
    try {
      val length = inputStream.readInt()
      if (length == 0) null
      else {
        val bytes = new Array[Byte](length)
        inputStream.readFully(bytes, 0, length)
        bytes
      }
    }
    catch {
      case npe: NullPointerException =>
        throw new EOFException("Connection closed.")
    }
  }

  @throws(classOf[IOException]) @throws(classOf[ClassNotFoundException])
  def readObject(inputStream: DataInputStream): AnyRef = {
    val metaData = readBytes(inputStream)
    val data = readBytes(inputStream)
    if (data eq null)
      Debug.error("Empty length message received")
    deserialize(if (metaData eq null) None else Some(metaData), data)
  }

  /** Writes bytes in the format [ int, bytes ]. Requires bytes != null */
  @throws(classOf[IOException])
  private def writeBytes(outputStream: DataOutputStream, bytes: Array[Byte]) {
    val length = bytes.length;
    outputStream.writeInt(length)
    outputStream.write(bytes, 0, length)
    outputStream.flush()
  }

  @throws(classOf[IOException])
  def writeObject(outputStream: DataOutputStream, obj: AnyRef) {
    serializeMetaData(obj) match {
      case Some(data) => writeBytes(outputStream, data)
      case None       => writeBytes(outputStream, Array[Byte]())
    }
    val bytes = serialize(obj)
    writeBytes(outputStream, bytes)
  }
}
