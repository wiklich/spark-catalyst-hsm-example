package br.com.wiklich.security.crypto

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/** ==DecryptWithKey==
  *
  * Companion object for the [[DecryptWithKeyExpression]] class.
  *
  * Provides utility methods to create Spark SQL expressions and DataFrame
  * columns that perform AES-GCM decryption using a predefined key from
  * [[br.com.wiklich.security.HsmSimulator]].
  *
  * This object allows the function to be used in both:
  *   - DataFrame API: via [[decryptWithKey]]
  *   - Catalyst expression API: via [[apply(Seq)]]
  */
object DecryptWithKey {

  /** Creates a new DataFrame column that decrypts base64-encoded input using
    * AES-GCM.
    *
    * This method is intended for use with the DataFrame DSL.
    *
    * @param inputCol
    *   A column containing base64-encoded encrypted data as string.
    * @return
    *   A new column representing the decrypted plaintext result.
    */
  def decryptWithKey(inputCol: Column): Column = {
    val expr = new DecryptWithKeyExpression(inputCol.expr)
    new Column(expr)
  }

  /** Factory method for creating a Catalyst expression that performs
    * decryption.
    *
    * This version is used internally by Spark SQL when parsing SQL queries
    * like:
    * {{{
    * SELECT decrypt_with_key('base64_string')
    * }}}
    *
    * Validates that exactly one argument is provided.
    *
    * @param children
    *   List of child expressions (must contain exactly one).
    * @return
    *   A new [[DecryptWithKeyExpression]] instance.
    * @throws IllegalArgumentException
    *   If number of arguments is not exactly one.
    */
  def apply(children: Seq[Expression]): Expression = {
    require(
      children.length == 1,
      s"decrypt_with_key() requires exactly one argument but got ${children.length}"
    )

    new DecryptWithKeyExpression(children.head)
  }
}
