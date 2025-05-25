package br.com.wiklich.security

import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets

/** ==HsmSimulator==
  *
  * A mock implementation of a Hardware Security Module (HSM) for development
  * and testing purposes.
  *
  * This object simulates secure key storage by providing predefined AES keys
  * mapped to string identifiers. In production environments, this should be
  * replaced with calls to a real HSM or secure key management system.
  *
  * Each key is derived from a fixed string and encoded as an AES key using
  * UTF-8 charset.
  *
  * ===Supported Key IDs===
  *   - `"key_name"` – for encrypting names
  *   - `"key_cpf"` – for encrypting CPF numbers
  *   - `"key_email"` – for encrypting email addresses
  *   - `"key_address"` – for encrypting address data
  *   - `"key_generic"` – for generic encryption use cases
  */
object HsmSimulator {

  /** A map of key identifiers to pre-defined AES secret keys used for
    * encryption/decryption.
    *
    * Keys are created from static strings for demonstration purposes only. The
    * keys are stored as [[SecretKeySpec]] objects for direct use in
    * cryptographic operations. Later, for editorial purposes, you can include
    * other keys.
    */
  private val keys = Map(
    "key_generic" -> "generic_key_1234"
  ).mapValues(k => new SecretKeySpec(k.getBytes(StandardCharsets.UTF_8), "AES"))

  /** Retrieves a secret key by its identifier.
    *
    * @param keyId
    *   The identifier of the key to retrieve.
    * @return
    *   The corresponding [[SecretKeySpec]] if found.
    * @throws IllegalArgumentException
    *   if the specified key ID is not recognized.
    */
  def getKey(keyId: String): SecretKeySpec = keys.getOrElse(
    keyId,
    throw new IllegalArgumentException(s"Unknown key: $keyId")
  )
}
