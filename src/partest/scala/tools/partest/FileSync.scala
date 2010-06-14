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

  private def flagFiles: Array[File] = {
    val files = System.getProperty("partest.waitfiles")
    if (files eq null)
      throw new Exception("No waitfiles found in partest.waitfiles property")
    files.split(",").map(new File(_)).toArray
  }

  private val timeout = 10000 // 10 seconds

  def waitFor(i: Int, waitTime: Int = timeout) {
    val files = flagFiles
    if (i < 0 || i >= files.size)
      throw new IllegalArgumentException("Bad file index: " + i)
    val file = files(i)
    val waitUntil = System.currentTimeMillis + waitTime 
    while (System.currentTimeMillis < waitUntil && !file.exists)
      Thread.sleep(500)
    if (!file.exists)
      Console.err.println("TIMEOUT waiting for Wait file " + file.getAbsolutePath) 
  }

}
