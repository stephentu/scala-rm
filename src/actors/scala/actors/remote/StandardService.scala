/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala.actors
package remote

class StandardService extends Service {
  override def serviceProviderFor(mode: ServiceMode.Value) = mode match {
    case ServiceMode.NonBlocking => new NonBlockingServiceProvider
    case ServiceMode.Blocking    => new BlockingServiceProvider
  }
}
