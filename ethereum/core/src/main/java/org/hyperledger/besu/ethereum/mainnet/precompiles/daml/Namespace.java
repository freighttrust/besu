/*
 * Copyright 2020 Blockchain Technology Partners.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.mainnet.precompiles.daml;

import org.hyperledger.besu.ethereum.core.Address;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey;
import com.google.protobuf.ByteString;
import org.apache.tuweni.bytes.Bytes;

/** Utility class dealing with DAML namespace functions and values. */
public final class Namespace {
  /** Sawtooth Namespaces are 6 chars long. */
  public static final int NAMESPACE_LENGTH = 6;

  /**
   * An ethereum address is 20 bytes represented in hexadecimal with a "0x" prefix, hence 42
   * characters in length
   */
  public static final int ADDRESS_STRING_LENGTH = Address.SIZE * 2;

  public static final int ADDRESS_HEX_STRING_LENGTH = ADDRESS_STRING_LENGTH + "0x".length();

  public static final String DAML_PUBLIC_ACCOUNT = Address.DAML_PUBLIC.toHexString();

  /** Family Version 1.0 . */
  public static final String DAML_FAMILY_VERSION_1_0 = "1.0";

  /** Address space for DamlStateValues. */
  public static final String DAML_NS_STATE_VALUE = getNameSpace() + "00";

  /** Address space for Log Entries. */
  public static final String DAML_NS_LOG_ENTRY = getNameSpace() + "01";

  /**
   * The first 6 characters of the family name hash.
   *
   * @return The first 6 characters of the family name hash
   */
  public static String getNameSpace() {
    return DAML_PUBLIC_ACCOUNT;
  }

  /**
   * Make a unique address comprised of a hashed root address and a count represented as an 8-byte
   * hexadecimal string padded with leading zeros.
   *
   * @param rootAddress the root address
   * @param count either the total number of parts about to be recorded in the ledger, or the number
   *     of the current part
   * @return the hash of the collected address
   */
  public static Bytes makeAddress(final String rootAddress, final int count) {
    String address = String.format("%s%08X", rootAddress, count);
    return Bytes.of(address.getBytes(Charset.defaultCharset()));
  }

  /**
   * Make a unique address given a namespace and data.
   *
   * @param ns the namespace byte string
   * @param data the data
   * @return the hash of the collected address
   */
  public static String makeAddress(final String ns, final ByteString data) {
    String hash = getHash(data);
    int begin = hash.length() - ADDRESS_HEX_STRING_LENGTH + ns.length() - 8;
    hash = hash.substring(begin);
    return ns + hash;
  }

  /**
   * Construct a context address for the ledger sync event with logical id eventId.
   *
   * @param entryId the log entry Id
   * @return the byte string address
   */
  protected static String makeDamlLogEntryAddress(final DamlLogEntryId entryId) {
    return makeAddress(DAML_NS_LOG_ENTRY, entryId.toByteString());
  }

  /**
   * Construct a context address for the given DamlStateKey.
   *
   * @param key DamlStateKey to be used for the address
   * @return the string address
   */
  protected static String makeDamlStateAddress(final DamlStateKey key) {
    return makeAddress(DAML_NS_STATE_VALUE, key.toByteString());
  }

  /**
   * Return the number of ledger entries that were used to store the value associated with the
   * supplied key.
   *
   * @param key ledger key
   * @return number of ledger entries associated with the key
   */
  public static int getLedgerEntryPartCount(final Bytes key) {
    return key.slice(ADDRESS_HEX_STRING_LENGTH - 8).toInt();
  }

  /**
   * For a given protocol buffer byte string return its SHA-512 hash.
   *
   * @param arg the string
   * @return the SHA-512 hash of the string
   */
  public static String getHash(final ByteString arg) {
    return getHash(arg.toByteArray());
  }

  /**
   * For a given byte array return its SHA-512 hash.
   *
   * @param arg the byte array
   * @return the SHA-512 hash of the byte array
   */
  @SuppressWarnings("DoNotInvokeMessageDigestDirectly")
  private static String getHash(final byte[] arg) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-512");
      digest.reset();
      digest.update(arg);
      return String.format("%0128x", new BigInteger(1, digest.digest()));
    } catch (NoSuchAlgorithmException nsae) {
      throw new RuntimeException("SHA-512 algorithm not found. This should never happen!", nsae);
    }
  }

  private Namespace() {}
}
