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

  def serialize(o: AnyRef): Array[Byte]
  def deserialize(c: String, a: Array[Byte]): AnyRef

  def serializeClassName(o: AnyRef): Array[Byte] = o.getClass.getName.getBytes

  /** Unique identifier used for this serializer */
  def uniqueId: Long

  @throws(classOf[IOException])
  private def readBytes(inputStream: DataInputStream): (String,Array[Byte]) = {
    try {
      val clazzLen = inputStream.readInt()
      //println("Received clazzLen = " + clazzLen)
      val clazzNameBytes = new Array[Byte](clazzLen)
      inputStream.readFully(clazzNameBytes, 0, clazzLen)
      val clazzName = new String(clazzNameBytes)
      //println("Received clazzName: " + clazzName)

      val length = inputStream.readInt()
      //println("Received length = " + length)
      val bytes = new Array[Byte](length)
      inputStream.readFully(bytes, 0, length)
      (clazzName,bytes)
    }
    catch {
      case npe: NullPointerException =>
        throw new EOFException("Connection closed.")
    }
  }

  @throws(classOf[IOException]) @throws(classOf[ClassNotFoundException])
  def readObject(inputStream: DataInputStream): AnyRef = {
    val (clazzName, bytes) = readBytes(inputStream)
    deserialize(clazzName, bytes)
  }

  @throws(classOf[IOException])
  private def writeBytes(outputStream: DataOutputStream, clazzName: String, bytes: Array[Byte]) {
    val clazzNameBytes = clazzName.getBytes
    val clazzNameLength = clazzNameBytes.length

    // [ int, varlen string ]
    outputStream.writeInt(clazzNameLength)
    outputStream.write(clazzNameBytes, 0, clazzNameLength)

    val length = bytes.length;

    // [ int, varlen bytes ]
    outputStream.writeInt(length)
    outputStream.write(bytes, 0, length)

    outputStream.flush()

    //print("writeBytes: clazzName " + clazzName)
    //print(", len = " + clazzNameLength)
    //println(", byteslen = " + length)
  }

  @throws(classOf[IOException])
  def writeObject(outputStream: DataOutputStream, obj: AnyRef) {
    //println("writeObject: obj = " + obj)
    val bytes = serialize(obj)
    writeBytes(outputStream, obj.getClass.getName, bytes)
  }
}
