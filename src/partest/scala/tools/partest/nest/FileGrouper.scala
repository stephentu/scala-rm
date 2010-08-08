package scala.tools.partest
package nest

import scala.collection.mutable.{ HashMap, ListBuffer }
import scala.util.matching.Regex
import java.io.File

trait ExtractorType

case object NoExtractor       extends ExtractorType
case object JvmGroupExtractor extends ExtractorType

case class FileGroup(val files: List[File], val tpe: ExtractorType, 
                     parentDir: Option[File], groupName: Option[String]) {
  def outDir  = new File(parentDir.get, groupName.get)
  def logFile = new File(parentDir.get, groupName.get + ".log")
}

/** 
 * This trait allows us to combine certain sets of files
 * into a FileGroup. This is useful for combining tests which
 * all need to be run at the same time
 */
trait FileGrouper {

  val kindGrouper = Map(
    /* For run kind, we group files by -jvmN extension. E.G. files
      * foo-jvm1.scala and foo-jvm2.scala will be in one group, and
      * files bar-jvm1.scala and bar-jvm2.scala will be in another
      * group */
    "run" -> (jvmGroupExtractor _, JvmGroupExtractor))

  private val jvmRegex = """(.+)-jvm\d+.scala""".r // TODO: should we require the .scala extension?
  private def jvmGroupExtractor(file: File): Option[String] = file.getName match {
    case jvmRegex(name) => Some(name)
    case _              => None
  }

  def groupFiles(files: List[File], kind: String): List[FileGroup] = kindGrouper.get(kind) match {
    case Some((func, tpe)) =>
      val groupMap  = new HashMap[(File,String), ListBuffer[File]]
      val untouched = new ListBuffer[File]
      files.foreach(file => {
        func(file) match {
          case Some(s) => 
            val key = (file.getParentFile, s)
            groupMap.get(key) match {
              case Some(buf) =>
                buf += file
              case None =>
                val newBuf = new ListBuffer[File]
                groupMap += (file.getParentFile, s) -> newBuf
                newBuf += file
            }
          case None =>
            untouched += file
        }
      })
      groupMap.map(t => { val ((p, g), l) = t; FileGroup(l.toList, tpe, Some(p), Some(g)) }).toList ::: 
        untouched.map(f => FileGroup(List(f), NoExtractor, None, None)).toList
    case None =>
      /* Default behavior is to place each file in its own group */
      files.map(f => FileGroup(List(f), NoExtractor, None, None))
  }
}
