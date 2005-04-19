/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2004, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |                                         **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
** $Id$
\*                                                                      */

package scala.xml;

import java.lang.StringBuffer; /* Java dependency! */
import scala.collection.mutable;
import scala.collection.immutable;
import scala.collection.Map;

/**
 * Utility functions for processing instances of bound and not bound XML 
 * classes, as well as escaping text nodes
 */
object Utility {

  def view(s: String): Text[String] = Text(s);

  /* escapes the characters &lt; &gt; &amp; and &quot; from string */
  def escape(text: String) = {
    val s = new StringBuffer();
    for (val c <- Iterator.fromString(text)) c match {
      case '<' => s.append("&lt;");
      case '>' => s.append("&gt;");
      case '&' => s.append("&amp;");
      case '"' => s.append("&quot;");
      case _   => s.append(c);
    }
    s.toString()
  }

  /**
   * Returns a set of all namespaces appearing in a node and all its 
   * descendants, including the empty namespaces
   *
   * @param node
  def collectNamespaces(node: Node): mutable.Set[String] = {
    collectNamespaces(node, new mutable.HashSet[String]());
  }
   */

  /**
   * Returns a set of all namespaces appearing in a sequence of nodes 
   * and all their descendants, including the empty namespaces
   *
   * @param nodes
  def collectNamespaces(nodes: Seq[Node]): mutable.Set[String] = {
    var m = new mutable.HashSet[String]();
    for (val n <- nodes) 
      collectNamespaces(n, m);
    m
  }
  
  private def collectNamespaces(node: Node, set: mutable.Set[String]): mutable.Set[String] = {
    def collect( n:Node ):Unit = {
      if( n.typeTag$ >= 0 ) {
        set += n.namespace;
        for (val a <- n.attributes)
          a.match {
            case _:PrefixedAttribute =>
              set += a.getNamespace(n)
            case _ =>
            }
        for (val i <- n.child) 
          collect(i);
      }
    }
    collect(node);
    set
  }
   */

  /**
   * A prefix mapping that maps the empty namespace to the empty prefix
   */
  val noPrefixes: Map[String,String] = 
    immutable.ListMap.Empty[String,String].update("","");

  /** string representation of an XML node, with comments stripped the comments
   * @see "toXML(Node, Boolean)"
   */
  def toXML(n: Node): String = toXML(n, true);

  /**
   * String representation of a Node. uses namespace mapping from
   * <code>defaultPrefixes(n)</code>.
   *
   * @param n
   * @param stripComment
   *
   * @todo define a way to escape literal characters to &amp;xx; references
   */
  def toXML(n: Node, stripComment: Boolean): String = {
    val sb = new StringBuffer();
    toXML(n, TopScope, sb, stripComment);
    sb.toString();
  }


  /** serializes a tree to the given stringbuffer
   *  with the given namespace prefix mapping.
   *  elements and attributes that have namespaces not in pmap are <strong>ignored</strong>
   *   @param n            the node
   *   @param pscope       the parent scope
   *   @param sb           stringbuffer to append to
   *   @param stripComment if true, strip comments
   */
  def toXML(x: Node, pscope: NamespaceBinding, sb: StringBuffer, stripComment: Boolean): Unit = {
    //Console.println("inside toXML, x.label = "+x.label);
    //Console.println("inside toXML, x.scope = "+x.scope);
    //Console.println("inside toXML, pscope = "+pscope);
    x match {

      case Text(t) => 
        sb.append(escape(t.toString()));

      case Comment(text) => 
        if (!stripComment) {
          sb.append("<!--");
          sb.append(text);
          sb.append("-->");
        }

      case _ if x.typeTag$ < 0 => 
        sb.append( x.toString() );
      
      case _  => { 
        // print tag with namespace declarations
        sb.append('<');
        x.nameToString(sb); //appendPrefixedName( x.prefix, x.label, pmap, sb );
        if (x.attributes != null) {
          x.attributes.toString(sb)
        }
        x.scope.toString(sb, pscope);
        sb.append('>');
        for (val c <- x.child.elements) {
          toXML(c, x.scope, sb, stripComment);
        }
        sb.append("</");
        x.nameToString(sb); //appendPrefixedName(x.prefix, x.label, pmap, sb);
        sb.append('>')
      }
    }
  }


  /** returns prefix of qualified name if any */
  final def prefix(name: String): Option[String] = {
    val i = name.indexOf(':');
    if( i != -1 ) Some( name.substring(0, i) ) else None
  }

  /**
   * Returns a hashcode for the given constituents of a node
   *
   * @param uri
   * @param label
   * @param attribHashCode
   * @param children
   */
  def hashCode(pre: String, label: String, attribHashCode: Int, scpeHash: Int, children: Seq[Node]) = {
    41 * pre.hashCode() % 7 + label.hashCode() * 53 + attribHashCode * 7 + scpeHash * 31 + children.hashCode()
  }

  /**
   * Returns a hashcode for the given constituents of a node
   *
   * @param uri
   * @param label
   * @param attribs
   * @param children
  def hashCode(uri: String, label: String, attribs: scala.collection.mutable.HashMap[Pair[String,String],String], scpe: Int, children: Seq[Node]): Int = {
    41 * uri.hashCode() % 7 + label.hashCode() + attribs.toList.hashCode() + scpe + children.hashCode()
  }
   */

  def systemLiteralToString(s: String) = {
    val sb = new StringBuffer("SYSTEM ");
    appendQuoted(s, sb);
    sb.toString();
  }

  def publicLiteralToString(s: String) = {
    val sb = new StringBuffer("PUBLIC ");
    sb.append('"').append(s).append('"');
    sb.toString();
  }

  /**
   * Appends &quot;s&quot; if s does not contain &quot;, &apos;s&apos;
   * otherwise
   *
   * @param s
   * @param sb
   */
  def appendQuoted(s: String, sb: StringBuffer) = {
    val ch = if (s.indexOf('"') == -1) '"' else '\'';
    sb.append(ch).append(s).append(ch)
  }

  /**
   * Appends &quot;s&quot; and escapes and &quot; i s with \&quot;
   *
   * @param s
   * @param sb
   */
  def appendEscapedQuoted(s: String, sb: StringBuffer) = {
    sb.append('"');
    val z:Seq[Char] = s;
    for( val c <- z ) c match {
      case '"' => sb.append('\\'); sb.append('"');
      case _   => sb.append( c );
    }
    sb.append('"')
  }

}
