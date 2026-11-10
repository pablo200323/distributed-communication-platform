package com.termuxbrokerdemo

import android.os.Build
import android.security.keystore.KeyProtection
import android.security.keystore.KeyProperties
import android.util.Base64
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import java.nio.charset.StandardCharsets
import java.security.KeyStore
import javax.crypto.AEADBadTagException
import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

class MessageSecurityModule(
    private val reactContext: ReactApplicationContext
) : ReactContextBaseJavaModule(reactContext) {

    override fun getName(): String {
        return "MessageSecurity"
    }

    @ReactMethod
    fun hasKey(keyId: String, promise: Promise) {
        try {
            promise.resolve(loadKeyStore().containsAlias(normalizeKeyId(keyId)))
        } catch (error: Exception) {
            promise.reject("KEY_LOOKUP_FAILED", error.message ?: "Failed to check key.", error)
        }
    }

    @ReactMethod
    fun importKey(keyId: String, base64Key: String, promise: Promise) {
        try {
            val normalizedKeyId = normalizeKeyId(keyId)
            val rawKey = Base64.decode(base64Key.trim(), Base64.DEFAULT)

            if (rawKey.size != 32) {
                promise.reject("INVALID_KEY_MATERIAL", "AES-256 key material must contain 32 bytes.")
                return
            }

            val secretKey = SecretKeySpec(rawKey, "AES")
            val keyStore = loadKeyStore()

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                val protection = KeyProtection.Builder(
                    KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
                )
                    .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                    .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                    .setRandomizedEncryptionRequired(true)
                    .build()

                keyStore.setEntry(
                    normalizedKeyId,
                    KeyStore.SecretKeyEntry(secretKey),
                    protection
                )
            } else {
                keyStore.setEntry(
                    normalizedKeyId,
                    KeyStore.SecretKeyEntry(secretKey),
                    null
                )
            }

            val result = Arguments.createMap()
            result.putBoolean("ok", true)
            result.putString("keyId", normalizedKeyId)
            promise.resolve(result)
        } catch (error: Exception) {
            promise.reject("KEY_IMPORT_FAILED", error.message ?: "Failed to import key.", error)
        }
    }

    @ReactMethod
    fun encrypt(keyId: String, plaintext: String, aad: String, promise: Promise) {
        try {
            val normalizedKeyId = normalizeKeyId(keyId)
            val key = getSecretKey(normalizedKeyId)
            val cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION)
            cipher.init(Cipher.ENCRYPT_MODE, key)
            val nonce = cipher.iv
            cipher.updateAAD(aad.toByteArray(StandardCharsets.UTF_8))

            val encryptedBytes = cipher.doFinal(plaintext.toByteArray(StandardCharsets.UTF_8))
            val ciphertextLength = encryptedBytes.size - TAG_LENGTH_BYTES

            if (ciphertextLength < 0) {
                promise.reject("ENCRYPTION_FAILED", "Encrypted output did not contain an authentication tag.")
                return
            }

            val ciphertext = encryptedBytes.copyOfRange(0, ciphertextLength)
            val tag = encryptedBytes.copyOfRange(ciphertextLength, encryptedBytes.size)

            val result = Arguments.createMap()
            result.putString("nonce", encodeBase64(nonce))
            result.putString("ciphertext", encodeBase64(ciphertext))
            result.putString("tag", encodeBase64(tag))
            promise.resolve(result)
        } catch (error: Exception) {
            if (error.message == UNKNOWN_KEY_ERROR) {
                promise.reject("UNKNOWN_KEY", UNKNOWN_KEY_ERROR)
            } else {
                promise.reject("ENCRYPTION_FAILED", error.message ?: "Failed to encrypt message.", error)
            }
        }
    }

    @ReactMethod
    fun decrypt(
        keyId: String,
        nonceBase64: String,
        ciphertextBase64: String,
        tagBase64: String,
        aad: String,
        promise: Promise
    ) {
        try {
            val normalizedKeyId = normalizeKeyId(keyId)
            val key = getSecretKey(normalizedKeyId)
            val nonce = Base64.decode(nonceBase64.trim(), Base64.DEFAULT)
            val ciphertext = Base64.decode(ciphertextBase64.trim(), Base64.DEFAULT)
            val tag = Base64.decode(tagBase64.trim(), Base64.DEFAULT)
            val encryptedBytes = ciphertext + tag

            val cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION)
            cipher.init(Cipher.DECRYPT_MODE, key, GCMParameterSpec(TAG_LENGTH_BITS, nonce))
            cipher.updateAAD(aad.toByteArray(StandardCharsets.UTF_8))

            val plaintextBytes = cipher.doFinal(encryptedBytes)
            promise.resolve(String(plaintextBytes, StandardCharsets.UTF_8))
        } catch (error: AEADBadTagException) {
            promise.reject("AUTHENTICATION_FAILED", "Encrypted content authentication failed.", error)
        } catch (error: Exception) {
            if (error.message == UNKNOWN_KEY_ERROR) {
                promise.reject("UNKNOWN_KEY", UNKNOWN_KEY_ERROR)
            } else {
                promise.reject("DECRYPTION_FAILED", error.message ?: "Failed to decrypt message.", error)
            }
        }
    }

    private fun normalizeKeyId(keyId: String): String {
        val normalizedKeyId = keyId.trim()

        if (normalizedKeyId.isEmpty()) {
            throw IllegalArgumentException("keyId must be a non-empty string")
        }

        return "$KEY_ALIAS_PREFIX$normalizedKeyId"
    }

    private fun loadKeyStore(): KeyStore {
        val keyStore = KeyStore.getInstance(ANDROID_KEY_STORE)
        keyStore.load(null)
        return keyStore
    }

    private fun getSecretKey(normalizedKeyId: String): SecretKey {
        val key = loadKeyStore().getKey(normalizedKeyId, null)

        if (key !is SecretKey) {
            throw IllegalStateException(UNKNOWN_KEY_ERROR)
        }

        return key
    }

    private fun encodeBase64(bytes: ByteArray): String {
        return Base64.encodeToString(bytes, Base64.NO_WRAP)
    }

    companion object {
        private const val ANDROID_KEY_STORE = "AndroidKeyStore"
        private const val KEY_ALIAS_PREFIX = "tfg-message-security:"
        private const val AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding"
        private const val TAG_LENGTH_BYTES = 16
        private const val TAG_LENGTH_BITS = 128
        private const val UNKNOWN_KEY_ERROR = "UNKNOWN_KEY"
    }
}
