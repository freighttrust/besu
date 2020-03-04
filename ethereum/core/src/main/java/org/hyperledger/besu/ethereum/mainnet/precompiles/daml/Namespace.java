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

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.ethereum.core.Address;

import java.math.BigInteger;

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey;
import com.google.protobuf.ByteString;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

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

  /** Enumeration that maps a DAML key type to a four-character DAML root address. */
  public enum DamlKeyType {
    /** DAML state value. */
    STATE,
    /** DAML log entry. */
    LOG;

    private final String rootAddress;

    private DamlKeyType() {
      rootAddress = String.format("%s%02d", getDamlAccountAddress(), ordinal());
    }

    /**
     * Return the 4-character DAML root address for this DAML key type.
     *
     * @return DAML root address
     */
    public String rootAddress() {
      return rootAddress;
    }
  }

  /**
   * Return the ethereum address of the DAML precompiled contract.
   *
   * @return ethereum address of DAML precompiled contract
   */
  public static String getDamlAccountAddress() {
    return DAML_PUBLIC_ACCOUNT;
  }

  /**
   * Make an ethereum storage slot address given a namespace and data.
   *
   * @param ns the namespace string
   * @param data the data
   * @return 256-bit ethereum storage slot address
   */
  public static UInt256 makeAddress(final DamlKeyType key, final byte[] data) {
    String hash = hashToString(getHash(data));

    // use only the last 28 bytes of the hash to allow room for the namespace
    final int begin = hash.length() - (STORAGE_SLOT_SIZE * 2) + key.rootAddress().length();
    hash = hash.substring(begin);
    return UInt256.fromHexString(key.rootAddress() + hash);
  }

  /**
   * Make an ethereum storage slot address given a namespace and data.
   *
   * @param ns the namespace string
   * @param data the data
   * @return 256-bit ethereum storage slot address
   */
  public static UInt256 makeAddress(final DamlKeyType key, final ByteString data) {
    return makeAddress(key, data.toByteArray());
  }

  /**
   * Make an ethereum storage slot address given a DAL state key.
   *
   * @param key DamlStateKey to be used for the address
   * @return the string address
   */
  public static UInt256 makeDamlStateAddress(final DamlStateKey key) {
    return makeAddress(DamlKeyType.STATE, key.toByteString());
  }

  /**
   * Make an ethereum storage slot address given a DAML log entry id.
   *
   * @param entryId the log entry Id
   * @return the byte string address
   */
  public static UInt256 makeDamlEntryIdAddress(final DamlLogEntryId entryId) {
    return makeAddress(DamlKeyType.LOG, entryId.toByteString());
  }

  /**
   * Return a SHA-256 hash of a byte array as a 32-byte Bytes wrapper.
   *
   * @param input the byte array
   * @return the SHA-256 hash of the byte array as a 64-character hexadecimal string
   */
  public static Bytes getHash(final byte[] input) {
    return Hash.sha256(Bytes.of(input));
  }

  /**
   * Return a 64-character hexadicaml string representation of a SHA-256 hash.
   *
   * @param input the byte array
   * @return the SHA-256 hash of the byte array as a 64-character hexadecimal string
   */
  private static String hashToString(final Bytes hash) {
    return String.format("%064x", new BigInteger(1, hash.toArray()));
  }

  private Namespace() {}
}
