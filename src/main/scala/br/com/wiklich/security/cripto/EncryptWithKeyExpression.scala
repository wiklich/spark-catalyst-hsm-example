package br.com.wiklich.security.crypto

import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import java.nio.charset.StandardCharsets
import java.security.SecureRandom

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.unsafe.types._
import org.apache.spark.sql.types._

import org.apache.spark.sql.catalyst.expressions.codegen.{
  CodegenContext,
  CodeGenerator,
  ExprCode
}

import br.com.wiklich.security.HsmSimulator

/** ==EncryptWithKeyExpression==
  *
  * A Spark Catalyst expression that performs AES-GCM encryption using a key
  * from [[HsmSimulator]].
  *
  * This expression encrypts plaintext strings using AES in GCM mode with a
  * randomly generated IV. The output is a Base64-encoded string containing:
  *   - 12-byte Initialization Vector (IV)
  *   - Followed by the GCM-encrypted ciphertext
  *
  * Supports both interpreted and code-generated evaluation for performance
  * optimization.
  */
case class EncryptWithKeyExpression(child: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes
    with NullIntolerant {

  /** Input data type validation. Only accepts strings to be encrypted.
    */
  override def inputTypes: Seq[DataType] = Seq(StringType)

  /** Output data type. Returns base64-encoded encrypted text as string.
    */
  override def dataType: DataType = StringType

  /** Retrieves the encryption key from HSM simulator. Lazy-loaded and cached
    * per instance.
    */
  @transient private lazy val key: SecretKeySpec =
    HsmSimulator.getKey("key_generic")

  /** Interpreted mode evaluation of the encryption logic.
    *
    * Generates a random IV, encrypts the UTF8 input string using AES-GCM, then
    * returns the concatenation of IV + cipherText encoded in Base64.
    *
    * Returns null if input is null or encryption fails.
    *
    * @param input
    *   Evaluated input value.
    * @return
    *   Base64-encoded encrypted string wrapped in UTF8String or null.
    */
  override def nullSafeEval(input: Any): Any = {
    if (input == null) null
    else {
      val plainText = input.asInstanceOf[UTF8String].toString
      val cipher = Cipher.getInstance("AES/GCM/NoPadding")
      val iv = new Array[Byte](12)
      val random = new SecureRandom()
      random.nextBytes(iv)

      cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, iv))
      val encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8))

      // Concatena IV + criptografado
      val fullEncrypted = Array.concat(iv, encrypted)
      UTF8String.fromString(
        java.util.Base64.getEncoder.encodeToString(fullEncrypted)
      )
    }
  }

  /** Code-generated version of the encryption logic.
    *
    * Optimized for execution speed inside Spark's Tungsten engine. Handles IV
    * generation, AES-GCM encryption, and Base64 encoding in Java bytecode.
    *
    * Catches exceptions and sets result to null on failure.
    *
    * @param ctx
    *   Code generation context.
    * @param ev
    *   Expression evaluation code.
    * @return
    *   Generated code block.
    */
  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = {
    val cipherClass = "javax.crypto.Cipher"
    val gcmSpecClass = "javax.crypto.spec.GCMParameterSpec"

    // Adds static function to initialize Cipher and load key
    val cipherGetInstance = ctx.addNewFunction(
      "cipherGetInstance",
      s"""
         |private static $cipherClass cipherGetInstance() {
         |   try {
         |     javax.crypto.spec.SecretKeySpec aesKey = new javax.crypto.spec.SecretKeySpec(
         |         br.com.wiklich.security.HsmSimulator.getKey("key_generic").getEncoded(),
         |         "AES"
         |     );
         |
         |     $cipherClass cipher = $cipherClass.getInstance("AES/GCM/NoPadding");
         |     return cipher;
         |   } catch (Exception e) {
         |     throw new RuntimeException(e);
         |   }
         |}
         |""".stripMargin
    )

    // Cache the key in mutable state to avoid re-fetching it multiple times
    val keyRef = ctx.addMutableState(
      "javax.crypto.spec.SecretKeySpec",
      "aesKey",
      v => s"""|$v = new javax.crypto.spec.SecretKeySpec(
      |     br.com.wiklich.security.HsmSimulator.getKey("key_generic").getEncoded(),
      |     "AES"
      |);
      |""".stripMargin
    )

    // Generate null-safe code for this expression
    nullSafeCodeGen(
      ctx,
      ev,
      { inputStr =>
        s"""
     |try {
     |  byte[] iv = new byte[12];
     |  new java.security.SecureRandom().nextBytes(iv);
     |
     |  $cipherClass cipher = $cipherClass.getInstance("AES/GCM/NoPadding");
     |  cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, $keyRef, new $gcmSpecClass(128, iv));
     |
     |  UTF8String inputUtf8 = ($inputStr);
     |  byte[] plainTextBytes = inputUtf8.getBytes();
     |  byte[] encrypted = cipher.doFinal(plainTextBytes);
     |
     |  byte[] fullEncrypted = new byte[iv.length + encrypted.length];
     |  System.arraycopy(iv, 0, fullEncrypted, 0, iv.length);
     |  System.arraycopy(encrypted, 0, fullEncrypted, iv.length, encrypted.length);
     |
     |  ${ev.value} = org.apache.spark.unsafe.types.UTF8String.fromString(
     |       java.util.Base64.getEncoder().encodeToString(fullEncrypted));
     |  ${ev.isNull} = false;
     |} catch (Exception e) {
     |  ${ev.isNull} = true;
     |}
     |""".stripMargin
      }
    )
  }

  /** SQL function name used in explain plans and UI.
    */
  override def prettyName: String = "encrypt_with_key"

  /** Creates a copy of this expression with a new child.
    *
    * @param newChild
    *   New child expression.
    * @return
    *   Copy of this expression with updated child.
    */
  override protected def withNewChildInternal(
      newChild: Expression
  ): EncryptWithKeyExpression =
    copy(child = newChild)
}
