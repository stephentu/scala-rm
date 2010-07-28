/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

trait MessageCreator[+P <: Proxy] {
  type MyNode                       <: Node
  type MyNamedSend                  <: NamedSend
  type MyLocator                    <: Locator
  type MyRemoteStartInvoke          <: RemoteStartInvoke
  type MyRemoteStartInvokeAndListen <: RemoteStartInvokeAndListen
  type MyRemoteApply                <: RemoteApply

  def newProxy(remoteNode: MyNode, name: Symbol): T 

  def newNode(address: String, port: Int): MyNode

  def newNamedSend(senderLoc: MyLocator, receiverLoc: MyLocator, metaData: Array[Byte], data: Array[Byte], session: Option[Symbol]): MyNamedSend

  def newLocator(node: MyNode, name: Symbol): MyLocator

  def newRemoteStartInvoke(actorClass: String): MyRemoteStartInvoke

  def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: Symbol, mode: ServiceMode.Value): MyRemoteStartInvokeAndListen

  def newRemoteApply(senderLoc: MyLocator, receiverLoc: MyLocator, rfun: RemoteFunction): MyRemoteApply

  def intercept(m: AnyRef): AnyRef = m match {
    case RemoteStartInvoke(actorClass) => 
      newRemoteStartInvoke(actorClass)
    case RemoteStartInvokeAndListen(actorClass, port, name, mode) =>
      newRemoteStartInvokeAndListen(actorClass, port, name, mode)
    case e => 
      e
  }
}

trait DefaultProxyCreator { this: MessageCreator[DefaultProxyImpl] =>
  override def newProxy(remoteNode: MyNode, name: Symbol): DefaultProxyImpl =
    new DefaultProxyImpl(remoteNode, name)
}

trait DefaultEnvelopeMessageCreator { this: MessageCreator[_ <: Proxy] =>
  override type MyNode        = DefaultNodeImpl
  override type MyNamedSend   = DefaultNamedSendImpl
  override type MyLocator     = DefaultLocatorImpl
  override type MyRemoteApply = DefaultRemoteApplyImpl

  override def newNode(address: String, port: Int): DefaultNodeImpl = DefaultNodeImpl(address, port)

  override def newNamedSend(senderLoc: DefaultLocatorImpl, receiverLoc: DefaultLocatorImpl, metaData: Array[Byte], data: Array[Byte], session: Option[Symbol]): DefaultNamedSendImpl =
    DefaultNamedSendImpl(senderLoc, receiverLoc, metaData, data, session)

  override def newLocator(node: DefaultNodeImpl, name: Symbol): DefaultLocatorImpl =
    DefaultLocatorImpl(node, name)

  override def newRemoteApply(senderLoc: DefaultLocatorImpl, receiverLoc: DefaultLocatorImpl, rfun: RemoteFunction): DefaultRemoteApplyImpl =
    DefaultRemoteApplyImpl(senderLoc, receiverLoc, rfun)
}

trait DefaultControllerMessageCreator { this: MessageCreator[_ <: Proxy] =>
  override type MyRemoteStartInvoke          = DefaultRemoteStartInvokeImpl
  override type MyRemoteStartInvokeAndListen = DefaultRemoteStartInvokeAndListenImpl

  override def newRemoteStartInvoke(actorClass: String): DefaultRemoteStartInvokeImpl = 
    DefaultRemoteStartInvokeImpl(actorClass)

  override def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: Symbol, mode: ServiceMode.Value): DefaultRemoteStartInvokeAndListenImpl =
    DefaultRemoteStartInvokeAndListenImpl(actorClass, port, name, mode)
}
