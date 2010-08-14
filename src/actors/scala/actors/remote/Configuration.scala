/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

/**
 * A <code>ConnectPolicy</code> describes how operations which require
 * establishing a TCP connection should operate, with respect to blocking the
 * current thread. This forms the basis for how all send operations, such as
 * <code>!</code>, <code>!!</code>, and <code>!?</code> on
 * <code>RemoteProxy</code> instances will act. A <code>ConnectPolicy</code>
 * is ordered in the sense that any guarantees that the previous level of
 * policy gives, is also guaranteed in the current level. <code>ConnectPolicy</code>
 * values are defined as follows:
 * <ul>
 *   <li>NoWait<br/>
 *     Do not block the current thread at all to establish a new connection.</li> 
 *   <li>WaitEstablished<br/>
 *     Wait until the TCP connection is established (in terms of the network
 *     layer. This validates that a remote node is listening (but does not
 *     validate anything else).</li>
 *   <li>WaitHandshake<br/>
 *     Wait until the Serializer handshake has completed (successfully or
 *     errorneously). This validates that a remote node is speaking the remote
 *     actors network protocol (with high probability).</li>
 *   <li>WaitVerified<br/>
 *     Wait until the remote end has sent back an ACK that the remote actor
 *     being communicated exists on the other end. Note that the consistency
 *     of this answer is determined by the <code>lookupValidPeriod</code>
 *     field of the <code>Configuration</code> instance.</li>
 * </ul>
 *
 * <b>Warning:</b> Care must be taken when using a <code>ConnectPolicy</code>
 * that is not <code>NoWait</code>. Because these operations will block the
 * current thread, care must be taken to ensure that the actor's do not
 * starve. 
 *
 * Note: This policy is currently not exposed to the user, but the
 * implementation is left in, in case this feature becomes desirable later on.
 * For now, every `Configuration` is configured with `NoWait` 
 *
 * @see Configuration
 * @see RemoteProxy
 */
private[remote] object ConnectPolicy extends Enumeration {
  val NoWait,              /** Don't wait at all for a connection to be established */
      WaitEstablished,     /** Wait until the network layer has established a TCP connection */
      WaitHandshake,       /** Wait until the serializers have successfully handshaked */
      WaitVerified = Value /** Wait until the remote actor has been located on the 
                             * other node, with an ACK returned */
}

/**
 * This object contains the implicit <code>Configuration</code> 
 * object which is the one used by default.
 */
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
 * trait, since a default one exists. In order to help make configuration less
 * verbose, there are helper traits <code>HasJavaSerializer</code>,
 * <code>HasBlockingMode</code>, <code>HasNonBlockingMode</code>, and
 * <code>HasDefaultMessageCreator</code> which can
 * be mixed-in as such:
 * {{{
 * implicit val config = new Configuration with HasNonBlockingMode
 *                                         with HasDefaultMessageCreator {
 *   // use the sandboxed class loader
 *   override def newSerializer = new JavaSerializer(classLoader) 
 * 
 *   override val numRetries    = 3   // Make 3 attempts to send messages before giving up 
 *
 *   override def classLoader   = ... // my special sandboxed class loader
 * }
 * }}}
 */
trait Configuration {

  /**
   * Contains the <code>ServiceMode</code> used when spawning a listener (via
   * <code>alive</code>)
   * 
   * @see alive
   */
  val aliveMode: ServiceMode.Value 

  /**
   * Contains the <code>ServiceMode</code> used when spawning a new connection
   * (via <code>select</code>, or <code>remoteActorAt</code>).
   *
   * @see select
   * @see remoteActorAt
   */
  val selectMode: ServiceMode.Value 

  /**
   * Contains the <code>ConnectPolicy</code> to utilize when attemping any 
   * network operations.<br/>
   * Default is <code>ConnectPolicy.NoWait</code>.
   */
  private[remote] val connectPolicy: ConnectPolicy.Value = ConnectPolicy.NoWait

  /**
   * The amount of time to cache name lookups for, per connection, in
   * milliseconds. Set to <code>0</code> to not cache any lookups (not
   * recommended).<br/>
   * Default is <code>30</code> minutes
   */
  val lookupValidPeriod: Long = 30 * 60 * 1000 /** 30 minutes */

  /**
   * Contains the time (in milliseconds) to wait for any blocking operations
   * to complete with respect to the connect policy.<br/>
   * Default is 10 seconds
   */
  val waitTimeout: Long = 10000 /** 10 seconds */

  /**
   * Returns a new <code>Serializer</code> to be used when spawning a new
   * connection. This method is called once for each unique
   * connection, so if the <code>Serializer</code> returned from this method
   * contains state, it should return a new instance each time (since it will
   * be used in a concurrent manner).<br/>
   *
   * <b>Implementation limitation</b>: Currently, <code>Serializer</code> instances
   * need to have a default no-arg constructor, in order to serialize a
   * <code>RemoteProxy</code> handle successfully. The
   * <code>JavaSerializer</code> satisfies this requirement.
   *
   * @see Serializer
   */
  def newSerializer(): Serializer

  /**
   * Returns a new <code>MessageCreator</code> to be used by the remote actor
   * start service.
   */
  def newMessageCreator(): MessageCreator

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
   * new instance of a class is needed. The only place this is currently used
   * is to create a new instance of an <code>Actor</code> when a remote start 
   * command is invoked.<br/>
   *
   * Note: The default implementation returns the current thread's context
   * class loader (via <code>currentThread.getContextClassLoader</code>).<br/>
   *
   * <b>Warning:</b> The reason that this field is configurable is a matter of security.
   * If there is a class for which you do not want to be instantiated, make
   * sure it is not accessible by this <code>ClassLoader</code>. A simple way
   * to do this is to create a white listing <code>ClassLoader</code>.
   *
   * @see ClassLoader
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
  with    HasDefaultMessageCreator
  with    HasBlockingMode 
  with    HasDefaultPolicy

/**
 * A default configuration for remote actors in <code>NonBlocking</code> mode.
 * Places both new connections and new listeners in <code>NonBlocking</code>
 * mode, and uses Java serialization as the <code>Serializer</code>. 
 * Does not override the default <code>classLoader</code>.
 */
class DefaultNonBlockingConfiguration
  extends Configuration
  with    HasJavaSerializer
  with    HasDefaultMessageCreator
  with    HasNonBlockingMode 
  with    HasDefaultPolicy

/**
 * A convenient mix-in to use Java serialization
 */
trait HasJavaSerializer { _: Configuration =>
  override def newSerializer() = new JavaSerializer(RemoteActor.classLoader)
}

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

/**
 * A convenient mix-in to use the default message creator
 */
trait HasDefaultMessageCreator { _: Configuration =>
  override def newMessageCreator() = new DefaultMessageCreator
}

/**
 * A convenient mix-in to use the default connect policy
 */
private[remote] trait HasDefaultPolicy { _: Configuration =>
  override val connectPolicy = ConnectPolicy.NoWait
}
