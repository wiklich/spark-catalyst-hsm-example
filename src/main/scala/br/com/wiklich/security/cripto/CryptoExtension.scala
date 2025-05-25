package br.com.wiklich.security.crypto

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

/** ==CryptoExtension==
  *
  * A Spark SQL extension that registers custom cryptographic UDFs
  * (`encrypt_with_key`, `decrypt_with_key`) into the Catalyst Optimizer.
  *
  * This allows users to call these functions directly in SQL queries or
  * DataFrame operations.
  *
  * The encryption and decryption logic uses AES-GCM, leveraging a predefined
  * key from [[HsmSimulator]].
  */
class CryptoExtension extends (SparkSessionExtensions => Unit) {

  /** Applies this extension by registering both encryption and decryption
    * functions.
    *
    * @param extensions
    *   The Spark session extensions registry to inject the functions into.
    */
  override def apply(extensions: SparkSessionExtensions): Unit = {
    registerEncryptFunction(extensions)
    registerDecryptFunction(extensions)
  }

  /** Registers the `encrypt_with_key` function in the Spark SQL environment.
    *
    * This function encrypts plaintext using AES-GCM with a fixed key retrieved
    * from [[HsmSimulator]]. It is designed to be called as a SQL expression.
    *
    * @param extensions
    *   The Spark session extensions registry.
    */
  private def registerEncryptFunction(
      extensions: SparkSessionExtensions
  ): Unit = {
    val expressionInfo = new ExpressionInfo(
      classOf[EncryptWithKeyExpression].getName,
      null,
      "encrypt_with_key",
      "_FUNC_(text) - Encrypts text using AES encryption with 'key_generic'.",
      """
        |Arguments:
        |  text - The plaintext string to encrypt.
      """.stripMargin,
      """
        |Examples:
        |  > SELECT encrypt_with_key('hello world');
        |   <base64 encrypted>
      """.stripMargin,
      """
        |Uses the predefined 'key_generic' from HSM simulator.
        |Returns NULL for NULL inputs.
      """.stripMargin,
      "misc_funcs",
      "3.5",
      "",
      "scala_udf"
    )

    extensions.injectFunction(
      (
        new FunctionIdentifier("encrypt_with_key"),
        expressionInfo,
        (children: Seq[Expression]) =>
          new EncryptWithKeyExpression(children.head)
      )
    )
  }

  /** Registers the `decrypt_with_key` function in the Spark SQL environment.
    *
    * This function decrypts base64-encoded ciphertext using AES-GCM with a
    * fixed key retrieved from [[HsmSimulator]]. It is designed to be called as
    * a SQL expression.
    *
    * @param extensions
    *   The Spark session extensions registry.
    */
  private def registerDecryptFunction(
      extensions: SparkSessionExtensions
  ): Unit = {
    val expressionInfo = new ExpressionInfo(
      classOf[DecryptWithKeyExpression].getName,
      null,
      "decrypt_with_key",
      "_FUNC_(text) - Decrypts text using AES encryption with 'key_generic'.",
      """
        |Arguments:
        |  text - The base64-encoded ciphertext string to decrypt.
      """.stripMargin,
      """
        |Examples:
        |  > SELECT decrypt_with_key(encrypt_with_key('Alice'));
        |   Alice
      """.stripMargin,
      """
        |Uses the predefined 'key_generic' from HSM simulator.
        |Returns NULL for invalid or NULL inputs.
      """.stripMargin,
      "misc_funcs",
      "3.5",
      "",
      "scala_udf"
    )

    extensions.injectFunction(
      (
        new FunctionIdentifier("decrypt_with_key"),
        expressionInfo,
        (children: Seq[Expression]) =>
          new DecryptWithKeyExpression(children.head)
      )
    )
  }
}
