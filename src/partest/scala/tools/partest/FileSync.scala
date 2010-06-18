/* NEST (New Scala Test)
 * Copyright 2007-2010 LAMP/EPFL
 * @author Philipp Haller
 */

// $Id$

package scala.tools.partest

import java.io.File

object FileSync {
  
  def writeFlag() {
    val flagFileName = System.getProperty("partest.flagfile")
    if (flagFileName eq null)
      throw new Exception("No flagfile found in partest.flagfile property")
    val flagFile = new File(flagFileName)
    if (flagFile.exists)
      Console.err.println("Flag file " + flagFileName + " already exists")
    else 
      flagFile.createNewFile()
  }

  private lazy val flagFiles: Array[File] = {
    val files = System.getProperty("partest.waitfiles")
    if (files eq null)
      throw new Exception("No waitfiles found in partest.waitfiles property")
    files.split(",").map(new File(_)).toArray
  }

  private val timeout = 10000 // 10 seconds

  private def checkIdx(i: Int) {
    val files = flagFiles
    if (i < 0 || i >= files.size)
      throw new IllegalArgumentException("Bad file index: " + i)
  }

  def waitFor(i: Int, waitTime: Int = timeout) {
    checkIdx(i)
    val file = flagFiles(i)
    val waitUntil = System.currentTimeMillis + waitTime 
    while (System.currentTimeMillis < waitUntil && !file.exists)
      Thread.sleep(500)
    if (!file.exists)
      Console.err.println("TIMEOUT waiting for Wait file " + file.getAbsolutePath) 
  }

  def waitForFiles(files: Array[Int], waitTime: Int = timeout) {
    files.foreach(i => waitFor(i, waitTime))
  }

}
