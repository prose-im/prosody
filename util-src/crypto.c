/* Prosody IM
-- Copyright (C) 2022 Matthew Wild
--
-- This project is MIT/X11 licensed. Please see the
-- COPYING file in the source package for more information.
--
*/

/*
* crypto.c
* Lua library for cryptographic operations using OpenSSL
*/

#include <string.h>
#include <stdlib.h>

#ifdef _MSC_VER
typedef unsigned __int32 uint32_t;
#else
#include <inttypes.h>
#endif

#include "lua.h"
#include "lauxlib.h"
#include <openssl/crypto.h>
#include <openssl/ecdsa.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/obj_mac.h>
#include <openssl/pem.h>

#if (LUA_VERSION_NUM == 501)
#define luaL_setfuncs(L, R, N) luaL_register(L, NULL, R)
#endif

#include "managed_pointer.h"

#define PKEY_MT_TAG "util.crypto key"

static BIO* new_memory_BIO() {
	return BIO_new(BIO_s_mem());
}

MANAGED_POINTER_ALLOCATOR(new_managed_EVP_MD_CTX, EVP_MD_CTX*, EVP_MD_CTX_new, EVP_MD_CTX_free)
MANAGED_POINTER_ALLOCATOR(new_managed_BIO_s_mem, BIO*, new_memory_BIO, BIO_free)
MANAGED_POINTER_ALLOCATOR(new_managed_EVP_CIPHER_CTX, EVP_CIPHER_CTX*, EVP_CIPHER_CTX_new, EVP_CIPHER_CTX_free)

static EVP_PKEY* pkey_from_arg(lua_State *L, int idx, const int type, const int require_private) {
	EVP_PKEY *pkey = *(EVP_PKEY**)luaL_checkudata(L, idx, PKEY_MT_TAG);
	int got_type;
	if(type || require_private) {
		lua_getuservalue(L, idx);
		if(type != 0) {
			lua_getfield(L, -1, "type");
			got_type = lua_tointeger(L, -1);
			if(got_type != type) {
				luaL_argerror(L, idx, "unexpected key type");
			}
			lua_pop(L, 1);
		}
		if(require_private != 0) {
			lua_getfield(L, -1, "private");
			if(lua_toboolean(L, -1) != 1) {
				luaL_argerror(L, idx, "private key expected, got public key only");
			}
			lua_pop(L, 1);
		}
		lua_pop(L, 1);
	}
	return pkey;
}

static int Lpkey_finalizer(lua_State *L) {
	EVP_PKEY *pkey = pkey_from_arg(L, 1, 0, 0);
	EVP_PKEY_free(pkey);
	return 0;
}

static int Lpkey_meth_get_type(lua_State *L) {
	EVP_PKEY *pkey = pkey_from_arg(L, 1, 0, 0);

	int key_type = EVP_PKEY_id(pkey);
	lua_pushstring(L, OBJ_nid2sn(key_type));
	return 1;
}

static int base_evp_sign(lua_State *L, const int key_type, const EVP_MD *digest_type) {
	EVP_PKEY *pkey = pkey_from_arg(L, 1, (key_type!=NID_rsassaPss)?key_type:NID_rsaEncryption, 1);
	luaL_Buffer sigbuf;

	size_t msg_len;
	const unsigned char* msg = (unsigned char*)lua_tolstring(L, 2, &msg_len);

	size_t sig_len;
	unsigned char *sig = NULL;
	EVP_MD_CTX *md_ctx = new_managed_EVP_MD_CTX(L);

	if(EVP_DigestSignInit(md_ctx, NULL, digest_type, NULL, pkey) != 1) {
		lua_pushnil(L);
		return 1;
	}
	if(key_type == NID_rsassaPss) {
		EVP_PKEY_CTX_set_rsa_padding(EVP_MD_CTX_pkey_ctx(md_ctx), RSA_PKCS1_PSS_PADDING);
	}
	if(EVP_DigestSign(md_ctx, NULL, &sig_len, msg, msg_len) != 1) {
		lua_pushnil(L);
		return 1;
	}

	// COMPAT w/ Lua 5.1
	luaL_buffinit(L, &sigbuf);
	sig = memset(luaL_prepbuffer(&sigbuf), 0, sig_len);

	if(EVP_DigestSign(md_ctx, sig, &sig_len, msg, msg_len) != 1) {
		lua_pushnil(L);
	}
	else {
		luaL_addsize(&sigbuf, sig_len);
		luaL_pushresult(&sigbuf);
		return 1;
	}

	return 1;
}

static int base_evp_verify(lua_State *L, const int key_type, const EVP_MD *digest_type) {
	EVP_PKEY *pkey = pkey_from_arg(L, 1, (key_type!=NID_rsassaPss)?key_type:NID_rsaEncryption, 0);

	size_t msg_len;
	const unsigned char *msg = (unsigned char*)luaL_checklstring(L, 2, &msg_len);

	size_t sig_len;
	const unsigned char *sig = (unsigned char*)luaL_checklstring(L, 3, &sig_len);

	EVP_MD_CTX *md_ctx = EVP_MD_CTX_new();

	if(EVP_DigestVerifyInit(md_ctx, NULL, digest_type, NULL, pkey) != 1) {
		lua_pushnil(L);
		goto cleanup;
	}
	if(key_type == NID_rsassaPss) {
		EVP_PKEY_CTX_set_rsa_padding(EVP_MD_CTX_pkey_ctx(md_ctx), RSA_PKCS1_PSS_PADDING);
	}
	int result = EVP_DigestVerify(md_ctx, sig, sig_len, msg, msg_len);
	if(result == 0) {
		lua_pushboolean(L, 0);
	} else if(result != 1) {
		lua_pushnil(L);
	}
	else {
		lua_pushboolean(L, 1);
	}
cleanup:
	EVP_MD_CTX_free(md_ctx);
	return 1;
}

static int Lpkey_meth_public_pem(lua_State *L) {
	char *data;
	size_t bytes;
	EVP_PKEY *pkey = pkey_from_arg(L, 1, 0, 0);
	BIO *bio = new_managed_BIO_s_mem(L);
	if(PEM_write_bio_PUBKEY(bio, pkey)) {
		bytes = BIO_get_mem_data(bio, &data);
		if (bytes > 0) {
			lua_pushlstring(L, data, bytes);
		}
		else {
			lua_pushnil(L);
		}
	}
	else {
		lua_pushnil(L);
	}
	return 1;
}

static int Lpkey_meth_private_pem(lua_State *L) {
	char *data;
	size_t bytes;
	EVP_PKEY *pkey = pkey_from_arg(L, 1, 0, 1);
	BIO *bio = new_managed_BIO_s_mem(L);

	if(PEM_write_bio_PrivateKey(bio, pkey, NULL, NULL, 0, NULL, NULL)) {
		bytes = BIO_get_mem_data(bio, &data);
		if (bytes > 0) {
			lua_pushlstring(L, data, bytes);
		}
		else {
			lua_pushnil(L);
		}
	}
	else {
		lua_pushnil(L);
	}
	return 1;
}

/* ecdsa_sha256_sign(key, data) */
static int Lecdsa_sha256_sign(lua_State *L) {
	return base_evp_sign(L, NID_X9_62_id_ecPublicKey, EVP_sha256());
}

/* ecdsa_sha256_verify(key, data, sig) */
static int Lecdsa_sha256_verify(lua_State *L) {
	return base_evp_verify(L, NID_X9_62_id_ecPublicKey, EVP_sha256());
}

static int push_pkey(lua_State *L, EVP_PKEY *pkey, const int type, const int privkey) {
	EVP_PKEY **ud = lua_newuserdata(L, sizeof(EVP_PKEY*));
	*ud = pkey;
	luaL_newmetatable(L, PKEY_MT_TAG);
	lua_setmetatable(L, -2);

	/* Set some info about the key and attach it as a user value */
	lua_newtable(L);
	if(type != 0) {
		lua_pushinteger(L, type);
		lua_setfield(L, -2, "type");
	}
	if(privkey != 0) {
		lua_pushboolean(L, 1);
		lua_setfield(L, -2, "private");
	}
	lua_setuservalue(L, -2);
	return 1;
}

static int Lgenerate_ed25519_keypair(lua_State *L) {
	EVP_PKEY *pkey = NULL;
	EVP_PKEY_CTX *pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, NULL);

	/* Generate key */
	EVP_PKEY_keygen_init(pctx);
	EVP_PKEY_keygen(pctx, &pkey);
	EVP_PKEY_CTX_free(pctx);

	push_pkey(L, pkey, NID_ED25519, 1);
	return 1;
}

static int Limport_private_pem(lua_State *L) {
	EVP_PKEY *pkey = NULL;

	size_t privkey_bytes;
	const char* privkey_data;
	BIO *bio = new_managed_BIO_s_mem(L);

	privkey_data = luaL_checklstring(L, 1, &privkey_bytes);
	BIO_write(bio, privkey_data, privkey_bytes);
	pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
	if (pkey) {
		push_pkey(L, pkey, EVP_PKEY_id(pkey), 1);
	}
	else {
		lua_pushnil(L);
	}

	return 1;
}

static int Limport_public_pem(lua_State *L) {
	EVP_PKEY *pkey = NULL;

	size_t pubkey_bytes;
	const char* pubkey_data;
	BIO *bio = new_managed_BIO_s_mem(L);

	pubkey_data = luaL_checklstring(L, 1, &pubkey_bytes);
	BIO_write(bio, pubkey_data, pubkey_bytes);
	pkey = PEM_read_bio_PUBKEY(bio, NULL, NULL, NULL);
	if (pkey) {
		push_pkey(L, pkey, EVP_PKEY_id(pkey), 0);
	}
	else {
		lua_pushnil(L);
	}

	return 1;
}

static int Led25519_sign(lua_State *L) {
	return base_evp_sign(L, NID_ED25519, NULL);
}

static int Led25519_verify(lua_State *L) {
	return base_evp_verify(L, NID_ED25519, NULL);
}

static int Lrsassa_pkcs1_256_sign(lua_State *L) {
	return base_evp_sign(L, NID_rsaEncryption, EVP_sha256());
}

static int Lrsassa_pkcs1_256_verify(lua_State *L) {
	return base_evp_verify(L, NID_rsaEncryption, EVP_sha256());
}

static int Lrsassa_pss_256_sign(lua_State *L) {
	return base_evp_sign(L, NID_rsassaPss, EVP_sha256());
}

static int Lrsassa_pss_256_verify(lua_State *L) {
	return base_evp_verify(L, NID_rsassaPss, EVP_sha256());
}

/* gcm_encrypt(key, iv, plaintext) */
static int Laes_gcm_encrypt(lua_State *L, const EVP_CIPHER *cipher, const unsigned char expected_key_len) {
	EVP_CIPHER_CTX *ctx;
	luaL_Buffer ciphertext_buffer;

	size_t key_len, iv_len, plaintext_len;
	int ciphertext_len, final_len;

	const unsigned char *key = (unsigned char*)luaL_checklstring(L, 1, &key_len);
	const unsigned char *iv = (unsigned char*)luaL_checklstring(L, 2, &iv_len);
	const unsigned char *plaintext = (unsigned char*)luaL_checklstring(L, 3, &plaintext_len);

	if(key_len != expected_key_len) {
		return luaL_error(L, "key must be %d bytes", expected_key_len);
	}
	luaL_argcheck(L, iv_len == 12, 2, "iv must be 12 bytes");
	if(lua_gettop(L) > 3) {
		return luaL_error(L, "Expected 3 arguments, got %d", lua_gettop(L));
	}

	// Create and initialise the context
	ctx = new_managed_EVP_CIPHER_CTX(L);

	// Initialise the encryption operation
	if(1 != EVP_EncryptInit_ex(ctx, cipher, NULL, NULL, NULL)) {
		return luaL_error(L, "Error while initializing encryption engine");
	}

	// Initialise key and IV
	if(1 != EVP_EncryptInit_ex(ctx, NULL, NULL, key, iv)) {
		return luaL_error(L, "Error while initializing key/iv");
	}

	luaL_buffinit(L, &ciphertext_buffer);
	unsigned char *ciphertext = (unsigned char*)luaL_prepbuffsize(&ciphertext_buffer, plaintext_len+16);

	if(1 != EVP_EncryptUpdate(ctx, ciphertext, &ciphertext_len, plaintext, plaintext_len)) {
		return luaL_error(L, "Error while encrypting data");
	}

	/*
	* Finalise the encryption. Normally ciphertext bytes may be written at
	* this stage, but this does not occur in GCM mode
	*/
	if(1 != EVP_EncryptFinal_ex(ctx, ciphertext + ciphertext_len, &final_len)) {
		return luaL_error(L, "Error while encrypting final data");
	}
	if(final_len != 0) {
		return luaL_error(L, "Non-zero final data");
	}

	/* Get the tag */
	if(1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_GET_TAG, 16, ciphertext + ciphertext_len)) {
		return luaL_error(L, "Unable to read AEAD tag of encrypted data");
	}

	luaL_addsize(&ciphertext_buffer, ciphertext_len + 16);
	luaL_pushresult(&ciphertext_buffer);

	return 1;
}

static int Laes_128_gcm_encrypt(lua_State *L) {
	return Laes_gcm_encrypt(L, EVP_aes_128_gcm(), 16);
}

static int Laes_256_gcm_encrypt(lua_State *L) {
	return Laes_gcm_encrypt(L, EVP_aes_256_gcm(), 32);
}

/* gcm_decrypt(key, iv, ciphertext) */
static int Laes_gcm_decrypt(lua_State *L, const EVP_CIPHER *cipher, const unsigned char expected_key_len) {
	EVP_CIPHER_CTX *ctx;
	luaL_Buffer plaintext_buffer;

	size_t key_len, iv_len, ciphertext_len;
	int plaintext_len, final_len;

	const unsigned char *key = (unsigned char*)luaL_checklstring(L, 1, &key_len);
	const unsigned char *iv = (unsigned char*)luaL_checklstring(L, 2, &iv_len);
	const unsigned char *ciphertext = (unsigned char*)luaL_checklstring(L, 3, &ciphertext_len);

	if(key_len != expected_key_len) {
		return luaL_error(L, "key must be %d bytes", expected_key_len);
	}
	luaL_argcheck(L, iv_len == 12, 2, "iv must be 12 bytes");
	luaL_argcheck(L, ciphertext_len > 16, 3, "ciphertext must be at least 16 bytes (including tag)");
	if(lua_gettop(L) > 3) {
		return luaL_error(L, "Expected 3 arguments, got %d", lua_gettop(L));
	}

	/* Create and initialise the context */
	ctx = new_managed_EVP_CIPHER_CTX(L);

	/* Initialise the decryption operation. */
	if(!EVP_DecryptInit_ex(ctx, cipher, NULL, NULL, NULL)) {
		return luaL_error(L, "Error while initializing decryption engine");
	}

	/* Initialise key and IV */
	if(!EVP_DecryptInit_ex(ctx, NULL, NULL, key, iv)) {
		return luaL_error(L, "Error while initializing key/iv");
	}

	luaL_buffinit(L, &plaintext_buffer);
	unsigned char *plaintext = (unsigned char*)luaL_prepbuffsize(&plaintext_buffer, ciphertext_len);

	/*
	* Provide the message to be decrypted, and obtain the plaintext output.
	* EVP_DecryptUpdate can be called multiple times if necessary
	*/
	if(!EVP_DecryptUpdate(ctx, plaintext, &plaintext_len, ciphertext, ciphertext_len-16)) {
		return luaL_error(L, "Error while decrypting data");
	}

	/* Set expected tag value. Works in OpenSSL 1.0.1d and later */
	if(!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_TAG, 16, (unsigned char*)ciphertext + (ciphertext_len-16))) {
		return luaL_error(L, "Error while processing authentication tag");
	}

	/*
	* Finalise the decryption. A positive return value indicates success,
	* anything else is a failure - the plaintext is not trustworthy.
	*/
	int ret = EVP_DecryptFinal_ex(ctx, plaintext + plaintext_len, &final_len);

	if(ret <= 0) {
		/* Verify failed */
		lua_pushnil(L);
		lua_pushliteral(L, "verify-failed");
		return 2;
	}

	luaL_addsize(&plaintext_buffer, plaintext_len + final_len);
	luaL_pushresult(&plaintext_buffer);
	return 1;
}

static int Laes_128_gcm_decrypt(lua_State *L) {
	return Laes_gcm_decrypt(L, EVP_aes_128_gcm(), 16);
}

static int Laes_256_gcm_decrypt(lua_State *L) {
	return Laes_gcm_decrypt(L, EVP_aes_256_gcm(), 32);
}

/* r, s = parse_ecdsa_sig(sig_der) */
static int Lparse_ecdsa_signature(lua_State *L) {
	ECDSA_SIG *sig;
	size_t sig_der_len;
	const unsigned char *sig_der = (unsigned char*)luaL_checklstring(L, 1, &sig_der_len);
	const BIGNUM *r, *s;
	luaL_Buffer rb, sb;
	int rlen, slen;

	sig = d2i_ECDSA_SIG(NULL, &sig_der, sig_der_len);

	if(sig == NULL) {
		lua_pushnil(L);
		return 1;
	}

	ECDSA_SIG_get0(sig, &r, &s);

	rlen = BN_num_bytes(r);
	slen = BN_num_bytes(s);

	// COMPAT w/ Lua 5.1
	#if LUAL_BUFFERSIZE < 32
	#error Configured LUAL_BUFFERSIZE is too small for this operation
	#endif

	luaL_buffinit(L, &rb);
	BN_bn2bin(r, (unsigned char*)luaL_prepbuffer(&rb));
	luaL_addsize(&rb, rlen);
	luaL_pushresult(&rb);

	luaL_buffinit(L, &sb);
	BN_bn2bin(s, (unsigned char*)luaL_prepbuffer(&sb));
	luaL_addsize(&sb, slen);
	luaL_pushresult(&sb);

	ECDSA_SIG_free(sig);

	return 2;
}

/* sig_der = build_ecdsa_signature(r, s) */
static int Lbuild_ecdsa_signature(lua_State *L) {
	ECDSA_SIG *sig = ECDSA_SIG_new();
	BIGNUM *r, *s;
	luaL_Buffer sigbuf;

	size_t rlen, slen;
	const unsigned char *rbin, *sbin;

	rbin = (unsigned char*)luaL_checklstring(L, 1, &rlen);
	sbin = (unsigned char*)luaL_checklstring(L, 2, &slen);

	r = BN_bin2bn(rbin, (int)rlen, NULL);
	s = BN_bin2bn(sbin, (int)slen, NULL);

	ECDSA_SIG_set0(sig, r, s);

	luaL_buffinit(L, &sigbuf);

	// COMPAT w/ Lua 5.1
	#if LUAL_BUFFERSIZE < 128
	#error Configured LUAL_BUFFERSIZE is too small for this operation
	#endif

	unsigned char *buffer = (unsigned char*)luaL_prepbuffer(&sigbuf);
	int len = i2d_ECDSA_SIG(sig, &buffer);
	luaL_addsize(&sigbuf, len);
	luaL_pushresult(&sigbuf);

	ECDSA_SIG_free(sig);

	return 1;
}

static const luaL_Reg Reg[] = {
	{ "ed25519_sign",                Led25519_sign             },
	{ "ed25519_verify",              Led25519_verify           },
	{ "rsassa_pkcs1_256_sign",       Lrsassa_pkcs1_256_sign    },
	{ "rsassa_pkcs1_256_verify",     Lrsassa_pkcs1_256_verify  },
	{ "rsassa_pss_256_sign",         Lrsassa_pss_256_sign      },
	{ "rsassa_pss_256_verify",       Lrsassa_pss_256_verify    },
	{ "aes_128_gcm_encrypt",         Laes_128_gcm_encrypt      },
	{ "aes_128_gcm_decrypt",         Laes_128_gcm_decrypt      },
	{ "aes_256_gcm_encrypt",         Laes_256_gcm_encrypt      },
	{ "aes_256_gcm_decrypt",         Laes_256_gcm_decrypt      },
	{ "ecdsa_sha256_sign",           Lecdsa_sha256_sign        },
	{ "ecdsa_sha256_verify",         Lecdsa_sha256_verify      },
	{ "generate_ed25519_keypair",    Lgenerate_ed25519_keypair },
	{ "import_private_pem",          Limport_private_pem       },
	{ "import_public_pem",           Limport_public_pem        },
	{ "parse_ecdsa_signature",       Lparse_ecdsa_signature    },
	{ "build_ecdsa_signature",       Lbuild_ecdsa_signature    },
	{ NULL,                          NULL                      }
};

static const luaL_Reg KeyMethods[] = {
	{ "private_pem",            Lpkey_meth_private_pem       },
	{ "public_pem",             Lpkey_meth_public_pem        },
	{ "get_type",               Lpkey_meth_get_type          },
	{ NULL,                     NULL                         }
};

static const luaL_Reg KeyMetatable[] = {
	{ "__gc",               Lpkey_finalizer },
	{ NULL,                 NULL            }
};

LUALIB_API int luaopen_util_crypto(lua_State *L) {
#if (LUA_VERSION_NUM > 501)
	luaL_checkversion(L);
#endif

	/* Initialize pkey metatable */
	luaL_newmetatable(L, PKEY_MT_TAG);
	luaL_setfuncs(L, KeyMetatable, 0);
	lua_newtable(L);
	luaL_setfuncs(L, KeyMethods, 0);
	lua_setfield(L, -2, "__index");
	lua_pop(L, 1);

	/* Initialize lib table */
	lua_newtable(L);
	luaL_setfuncs(L, Reg, 0);
	lua_pushliteral(L, "-3.14");
	lua_setfield(L, -2, "version");
#ifdef OPENSSL_VERSION
	lua_pushstring(L, OpenSSL_version(OPENSSL_VERSION));
	lua_setfield(L, -2, "_LIBCRYPTO_VERSION");
#endif
	return 1;
}
