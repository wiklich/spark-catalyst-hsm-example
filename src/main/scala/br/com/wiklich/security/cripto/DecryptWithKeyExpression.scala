package br.com.wiklich.security.crypto

import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.unsafe.types._
import org.apache.spark.sql.types._

import org.apache.spark.sql.catalyst.expressions.codegen.{
  CodegenContext,
  CodeGenerator,
  ExprCode
}

import br.com.wiklich.security.HsmSimulator

/** ==DecryptWithKeyExpression==
  *
  * A Spark Catalyst expression that performs AES-GCM decryption using a key
  * from [[HsmSimulator]].
  *
  * This expression expects base64-encoded input containing:
  *   - 12-byte Initialization Vector (IV)
  *   - Followed by the GCM-encrypted ciphertext
  *
  * If decryption fails or input is invalid, returns `null`.
  *
  * Supports both interpreted and code-generated evaluation for performance
  * optimization.
  */
case class DecryptWithKeyExpression(child: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes
    with NullIntolerant {

  /** Input data type validation. Only accepts strings (assumed to be
    * base64-encoded).
    */
  override def inputTypes: Seq[DataType] = Seq(StringType)

  /** Output data type. Returns decrypted text as string. */
  override def dataType: DataType = StringType

  /** Retrieves the encryption key from HSM simulator. Lazy-loaded and cached
    * per instance.
    */
  @transient private lazy val key: SecretKeySpec =
    HsmSimulator.getKey("key_generic")

  /** Interpreted mode evaluation of the decryption logic.
    *
    * Converts input UTF8String to byte array, decodes Base64, extracts IV and
    * cipher text, then decrypts using AES-GCM.
    *
    * Returns null if:
    *   - Input is null
    *   - Data is too short to contain IV
    *   - Decryption fails
    *
    * @param input
    *   Evaluated input value.
    * @return
    *   Decrypted string wrapped in UTF8String or null.
    */
  override def nullSafeEval(input: Any): Any = {
    try {
      val encoded = input.asInstanceOf[UTF8String].toString
      val fullData = java.util.Base64.getDecoder.decode(encoded)

      if (fullData.length < 12) return null

      val iv = new Array[Byte](12)
      System.arraycopy(fullData, 0, iv, 0, iv.length)

      val cipher = Cipher.getInstance("AES/GCM/NoPadding")
      cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, iv))

      val cipherText = new Array[Byte](fullData.length - 12)
      System.arraycopy(fullData, 12, cipherText, 0, cipherText.length)

      val decrypted = cipher.doFinal(cipherText)
      UTF8String.fromString(new String(decrypted, StandardCharsets.UTF_8))
    } catch {
      case _: Exception => null
    }
  }

  /** Code-generated version of the decryption logic.
    *
    * Optimized for execution speed inside Spark's Tungsten engine. Handles
    * Base64 decoding, IV extraction, and AES-GCM decryption in Java bytecode.
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
     |  UTF8String inputUtf8 = ($inputStr);
     |  byte[] fullData = java.util.Base64.getDecoder().decode(inputUtf8.getBytes());
     |
     |  if (fullData == null || fullData.length < 12) {
     |    ${ev.isNull} = true;
     |  } else {
     |    byte[] iv = new byte[12];
     |    System.arraycopy(fullData, 0, iv, 0, 12);
     |
     |    $cipherClass cipher = $cipherClass.getInstance("AES/GCM/NoPadding");
     |    cipher.init(javax.crypto.Cipher.DECRYPT_MODE, $keyRef, new $gcmSpecClass(128, iv));
     |
     |    int cipherLength = fullData.length - 12;
     |    byte[] cipherText = new byte[cipherLength];
     |    System.arraycopy(fullData, 12, cipherText, 0, cipherLength);
     |
     |    byte[] decrypted = cipher.doFinal(cipherText);
     |    ${ev.value} = org.apache.spark.unsafe.types.UTF8String.fromBytes(decrypted);
     |    ${ev.isNull} = false;
     |  }
     |} catch (Exception e) {
     |  ${ev.isNull} = true;
     |}
     |""".stripMargin
      }
    )
  }

  /** SQL function name used in explain plans and UI.
    */
  override def prettyName: String = "decrypt_with_key"

  /** Creates a copy of this expression with a new child.
    *
    * @param newChild
    *   New child expression.
    * @return
    *   Copy of this expression with updated child.
    */
  override protected def withNewChildInternal(
      newChild: Expression
  ): DecryptWithKeyExpression =
    copy(child = newChild)
}
