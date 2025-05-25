package br.com.wiklich.security.crypto

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/** ==EncryptWithKey==
  *
  * Companion object for the [[EncryptWithKeyExpression]] class.
  *
  * Provides utility methods to create Spark SQL expressions and DataFrame
  * columns that perform AES-GCM encryption using a predefined key from
  * [[br.com.wiklich.security.HsmSimulator]].
  *
  * This object allows the function to be used in both:
  *   - DataFrame API: via [[encryptWithKey]]
  *   - Catalyst expression API: via [[apply(Seq)]]
  */
object EncryptWithKey {

  /** Creates a new DataFrame column that encrypts input using AES-GCM.
    *
    * This method is intended for use with the DataFrame DSL.
    *
    * @param inputCol
    *   A column containing plaintext string data to be encrypted.
    * @return
    *   A new column representing the base64-encoded encrypted result.
    */
  def encryptWithKey(inputCol: Column): Column = {
    val expr = new EncryptWithKeyExpression(inputCol.expr)
    new Column(expr)
  }

  /** Factory method for creating a Catalyst expression that performs
    * encryption.
    *
    * This version is used internally by Spark SQL when parsing SQL queries
    * like:
    * {{{
    * SELECT encrypt_with_key('hello world')
    * }}}
    *
    * Validates that exactly one argument is provided.
    *
    * @param children
    *   List of child expressions (must contain exactly one).
    * @return
    *   A new [[EncryptWithKeyExpression]] instance.
    * @throws IllegalArgumentException
    *   if number of arguments is not exactly one.
    */
  def apply(children: Seq[Expression]): Expression = {
    require(
      children.length == 1,
      s"encrypt_with_key() requires exactly one argument but got ${children.length}"
    )

    new EncryptWithKeyExpression(children.head)
  }
}
