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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey;
import com.google.protobuf.ByteString;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Utility class dealing with DAML namespace functions and values. */
public final class Namespace {
  /**
   * An ethereum address is 20 bytes represented as a hexadecimal string with a "0x" prefix, hence
   * 42 characters in length.
   */
  public static final int ADDRESS_STRING_LENGTH = Address.SIZE * 2;

  public static final int ADDRESS_HEX_STRING_LENGTH = ADDRESS_STRING_LENGTH + "0x".length();

  /** The size of an ethereum storage slot. */
  public static final int STORAGE_SLOT_SIZE = Bytes32.SIZE;

  /** The ethereum address of the DAML precompiled contract. */
  public static final String DAML_PUBLIC_ACCOUNT =
      String.format("%02x", Address.DAML_PUBLIC.toBigInteger());

  /** Address space for DamlStateValues. */
  public static final String DAML_NS_STATE_VALUE = getDamlKeyRoot("00");

  /** Address space for Log Entries. */
  public static final String DAML_NS_LOG_ENTRY = getDamlKeyRoot("01");

  /**
   * Return the ethereum address of the DAML precompiled contract.
   *
   * @return ethereum address of DAML precompiled contract
   */
  public static String getDamlAccountAddress() {
    return DAML_PUBLIC_ACCOUNT;
  }

  /**
   * Return the first four characters of a DAML storage slot key in ethereum given the key type, one
   * of state key or log id.
   *
   * @param keyType key type, one of state key or log id
   * @return first four characters of DAML storage slot key for this key type
   */
  public static String getDamlKeyRoot(final String keyType) {
    return String.format("%s%s", getDamlAccountAddress(), keyType);
  }

  /**
   * Make an ethereum storage slot address given a namespace and data.
   *
   * @param ns the namespace string
   * @param data the data
   * @return 256-bit ethereum storage slot address
   */
  public static Bytes makeAddress(final String ns, final ByteString data) {
    String hash = getHash(data);

    // use only the last 28 bytes of the hash to allow room for the namespace
    final int begin = hash.length() - STORAGE_SLOT_SIZE + ns.length();
    hash = hash.substring(begin);
    return Bytes32.fromHexString(ns + hash);
  }

  /**
   * Make an ethereum storage slot address given a DAML log entry id.
   *
   * @param entryId the log entry Id
   * @return the byte string address
   */
  protected static Bytes makeDamlLogEntryAddress(final DamlLogEntryId entryId) {
    return makeAddress(DAML_NS_LOG_ENTRY, entryId.toByteString());
  }

  /**
   * Make an ethereum storage slot address given a DAL state key.
   *
   * @param key DamlStateKey to be used for the address
   * @return the string address
   */
  protected static Bytes makeDamlStateAddress(final DamlStateKey key) {
    return makeAddress(DAML_NS_STATE_VALUE, key.toByteString());
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
