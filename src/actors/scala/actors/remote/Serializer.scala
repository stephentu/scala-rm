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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, 
                DataOutputStream, EOFException, IOException, ObjectInputStream,
                ObjectOutputStream}

class IllegalHandshakeStateException extends Exception

abstract class Serializer {

  var service: Service = _   

  // Handshake management 

  /** 
   * Starting state of the handshake. 
   * Return None if no handshake is desired, in which case the next two
   * methods will never be called (and can thus return null)
   */
  val initialState: Option[Any]

  /**
   * Returns the next message to send to the other side in the handshake.
   * The input is the current state, output is a tuple of 
   * (next state, next message). Next message is None to signal completion
   * of handshake.
   */
  def nextHandshakeMessage: PartialFunction[Any, (Any, Option[Any])]

  /**
   * Callback to receive the next message of the handshake from the other side.
   * Input is a tuple of (current state, message to handle). Output is the
   * next state transition.
   */
  def handleHandshakeMessage: PartialFunction[(Any, Any), Any]

  def isHandshakeError(m: Any) = !(nextHandshakeMessage isDefinedAt m)

  def isHandshakeError(m: (Any, Any)) = !(handleHandshakeMessage isDefinedAt m)

  // Message serialization

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

  /** 
   * Default equality method for serializers. Looks simply at the uniqueId field 
   */
  override def equals(o: Any): Boolean = o match {
    case s: Serializer => uniqueId == s.uniqueId
    case _             => false
  }

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

  private def readObjectBytes(inputStream: DataInputStream)(f: (Array[Byte], Array[Byte]) => AnyRef) = {
    val metaData = readBytes(inputStream)
    val data = readBytes(inputStream)
    if (data eq null)
      Debug.error("Empty length message received")
    f(metaData, data)
  }

  @throws(classOf[IOException]) 
  @throws(classOf[ClassNotFoundException])
  def readObject(inputStream: DataInputStream): AnyRef = readObjectBytes(inputStream) {
    (metaData, data) => deserialize(if (metaData eq null) None else Some(metaData), data)
  }

  /** Writes bytes in the format [ int, bytes ]. Requires bytes != null */
  @throws(classOf[IOException])
  private def writeBytes(outputStream: DataOutputStream, bytes: Array[Byte]) {
    val length = bytes.length;
    outputStream.writeInt(length)
    outputStream.write(bytes, 0, length)
    outputStream.flush()
  }

  private def writeObjectBytes(out: DataOutputStream)(f: => (Array[Byte], Array[Byte])) {
    val (meta, data) = f
    writeBytes(out, meta)
    writeBytes(out, data)
  }

  @throws(classOf[IOException])
  def writeObject(outputStream: DataOutputStream, obj: AnyRef) {
    writeObjectBytes(outputStream) {
      val meta = serializeMetaData(obj) match {
        case Some(data) => data
        case None       => Array[Byte]()
      }
      val data = serialize(obj)
      (meta, data)
    }
  }

  // Default java serialization methods to use during handshake process

  def readJavaObject(inp: DataInputStream) = readObjectBytes(inp) {
    (meta, data) => javaDeserialize(None, data)
  }

  def writeJavaObject(out: DataOutputStream, obj: AnyRef) {
    writeObjectBytes(out) {
      (Array[Byte](), javaSerialize(obj))
    }
  }

  def javaSerialize(o: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(o)
    out.flush()
    bos.toByteArray()
  }

  def javaDeserialize(metaData: Option[Array[Byte]], bytes: Array[Byte]): AnyRef = {
    val bis = new ByteArrayInputStream(bytes)
    val in  = new ObjectInputStream(bis)
    in.readObject()
  }

}
