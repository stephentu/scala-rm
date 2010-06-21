/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

import java.net.{ InetSocketAddress, Socket }

trait NodeImplicits {

  implicit def nodeToInetAddress(node: Node): InetSocketAddress = 
    node.toInetSocketAddress

  implicit def inetAddressToNode(addr: InetSocketAddress): Node =
    Node(addr.getHostName, addr.getPort)

  /** Implicitly converts socket to REMOTE node, not local */ 
  implicit def socketToInetSocketAddress(socket: Socket): InetSocketAddress = 
    new InetSocketAddress(socket.getInetAddress, socket.getPort)

  implicit def socketToNode(socket: Socket): Node =
    inetAddressToNode(socketToInetSocketAddress(socket))

}
