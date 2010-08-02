/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2005-2010, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package scala.actors
package remote

trait MessageCreator {
  //type MyAsyncSend                  <: AsyncSend
  //type MySyncSend                   <: SyncSend
  //type MySyncReply                  <: SyncReply
  //type MyRemoteStartInvoke          <: RemoteStartInvoke
  //type MyRemoteStartInvokeAndListen <: RemoteStartInvokeAndListen
  //type MyRemoteApply                <: RemoteApply

  def newAsyncSend(senderName: Option[Symbol], receiverName: Symbol, metaData: Array[Byte], data: Array[Byte]): /* MyAsyncSend */ AnyRef

  def newSyncSend(senderName: Symbol, receiverName: Symbol, metaData: Array[Byte], data: Array[Byte], session: Symbol): /* MySyncSend */ AnyRef

  def newSyncReply(receiverName: Symbol, metaData: Array[Byte], data: Array[Byte], session: Symbol): /* MySyncReply */ AnyRef

  def newRemoteStartInvoke(actorClass: String): /* MyRemoteStartInvoke */ AnyRef

  def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: Symbol): /* MyRemoteStartInvokeAndListen */ AnyRef

  def newRemoteApply(senderName: Symbol, receiverName: Symbol, rfun: RemoteFunction): /* MyRemoteApply */ AnyRef

  protected def doIntercept: PartialFunction[AnyRef, AnyRef] = {
    case RemoteStartInvoke(actorClass) => 
      newRemoteStartInvoke(actorClass)
    case RemoteStartInvokeAndListen(actorClass, port, name) =>
      newRemoteStartInvokeAndListen(actorClass, port, name)
  }

  def intercept(m: AnyRef) =
    if (doIntercept.isDefinedAt(m)) 
      doIntercept(m)
    else
      m
}

trait DefaultEnvelopeMessageCreator { this: MessageCreator =>
  //override type MyAsyncSend   = DefaultAsyncSendImpl
  //override type MySyncSend    = DefaultSyncSendImpl
  //override type MySyncReply   = DefaultSyncReplyImpl
  //override type MyRemoteApply = DefaultRemoteApplyImpl

  override def newAsyncSend(senderName: Option[Symbol], receiverName: Symbol, metaData: Array[Byte], data: Array[Byte]) =
    DefaultAsyncSendImpl(senderName, receiverName, metaData, data)

  override def newSyncSend(senderName: Symbol, receiverName: Symbol, metaData: Array[Byte], data: Array[Byte], session: Symbol) =
    DefaultSyncSendImpl(senderName, receiverName, metaData, data, session)
                                                                                   
  override def newSyncReply(receiverName: Symbol, metaData: Array[Byte], data: Array[Byte], session: Symbol) = DefaultSyncReplyImpl(receiverName, metaData, data, session)

  override def newRemoteApply(senderName: Symbol, receiverName: Symbol, rfun: RemoteFunction) =
    DefaultRemoteApplyImpl(senderName, receiverName, rfun)
}

trait DefaultControllerMessageCreator { this: MessageCreator =>
  //override type MyRemoteStartInvoke          = DefaultRemoteStartInvokeImpl
  //override type MyRemoteStartInvokeAndListen = DefaultRemoteStartInvokeAndListenImpl

  override def newRemoteStartInvoke(actorClass: String) = 
    DefaultRemoteStartInvokeImpl(actorClass)

  override def newRemoteStartInvokeAndListen(actorClass: String, port: Int, name: Symbol) =
    DefaultRemoteStartInvokeAndListenImpl(actorClass, port, name)
}
