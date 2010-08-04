/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

object Configuration {
  /**
   * This object is the default configuration in scope.
   */
  implicit object DefaultConfig extends DefaultConfiguration
}

/**
 * This trait is responsible for containing the parameters necessary to
 * configure network activities for remote actors. Users not wishing to
 * customize the behavior of remote actors do not need to worry about this
 * trait, since a default one exists.
 */
trait Configuration {

  /**
   * Contains the <code>ServiceMode</code> used when spawning a listener (via
   * <code>alive</code>)
   */
  val aliveMode: ServiceMode.Value 

  /**
   * Contains the <code>ServiceMode</code> used when spawning a new connection
   * (via <code>select</code>).
   */
  val selectMode: ServiceMode.Value 

  /**
   * Returns a new <code>Serializer</code> to be used when spawning a new
   * connection. Note that this <code>Serializer<code> must be locatable by
   * the remote node's <code>ClassLoader</code> and must have a no argument 
   * constructor. The <code>Serializer</code> returned here must be the one to
   * use on the client side. This method is called once for each unique
   * connection, so if the <code>Serializer</code> returned from this method
   * contains state, it should return a new instance each time.
   */
  def newSerializer(): Serializer

  /**
   * Contains the number of retries that should be automatically attempted (ie
   * without user intervention) when attempting to deliver a message via the
   * network before an exception is thrown. 
   *
   * Note: The default is <code>0</code> attempts (meaning exactly one try
   * is made, and if it fails, an exception is thrown).
   */
  val numRetries: Int = 0

  /**
   * Returns the <code>ClassLoader</code> used to locate classes whenever a
   * new instance of a class is needed. The two places this is currently used
   * are (1) to create a new instance of a <code>Serializer</code> on the
   * remote end when a connection is spawned, and (2) to create a new instance
   * of an <code>Actor</code> when a remote start command is invoked.
   *
   * Note: The default implementation returns the current thread's context
   * class loader (via <code>currentThread.getContextClassLoader</code>).
   *
   * WARNING: The reason that this field is configurable is a matter of security.
   * If there is a class for which you do not want to be instantiated, make
   * sure it is not accessible by this <code>ClassLoader</code>. A simple way
   * to do this is to create a white listing <code>ClassLoader</code>.
   */
  def classLoader: ClassLoader = currentThread.getContextClassLoader

  private[remote] lazy val cachedSerializer: Serializer = newSerializer()
}

/**
 * The default configuration for remote actors. Places both new connections
 * and new listeners in <code>Blocking</code> mode, and uses Java
 * serialization as the <code>Serializer</code>. Does not override the default
 * <code>classLoader</code>.
 */
class DefaultConfiguration 
  extends Configuration
  with    HasJavaSerializer
  with    HasBlockingMode 

/**
 * A default configuration for remote actors in <code>NonBlocking</code> mode.
 * Places both new connections and new listeners in <code>NonBlocking</code>
 * mode, and uses Java serialization as the <code>Serializer</code>. 
 * Does not override the default <code>classLoader</code>.
 */
class DefaultNonBlockingConfiguration
  extends Configuration
  with    HasJavaSerializer
  with    HasNonBlockingMode 

/**
 * A convenient mix-in to use Java serialization
 */
trait HasJavaSerializer { _: Configuration =>
  override def newSerializer() = new JavaSerializer(RemoteActor.classLoader)
}

trait DefaultMessageCreator extends MessageCreator
                            with    DefaultEnvelopeMessageCreator 
                            with    DefaultControllerMessageCreator

/**
 * A convenient mix-in to use blocking mode
 */
trait HasBlockingMode { _: Configuration =>
  override val aliveMode  = ServiceMode.Blocking
  override val selectMode = ServiceMode.Blocking
}

/**
 * A convenient mix-in to use non blocking mode
 */
trait HasNonBlockingMode { _: Configuration =>
  override val aliveMode  = ServiceMode.NonBlocking
  override val selectMode = ServiceMode.NonBlocking
}
