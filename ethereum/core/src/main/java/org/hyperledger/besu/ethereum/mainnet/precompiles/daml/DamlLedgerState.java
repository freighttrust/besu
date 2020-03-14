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

import org.hyperledger.besu.ethereum.core.AccountStorageEntry;
import org.hyperledger.besu.ethereum.core.MutableAccount;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlCommandDedupValue;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateValue;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.daml.ledger.participant.state.v1.TimeModel;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class DamlLedgerState implements LedgerState {
  private static final Logger LOG = LogManager.getLogger();

  private final MutableAccount account;

  public DamlLedgerState(final MutableAccount theAccount) {
    this.account = theAccount;
  }

  @Override
  public DamlStateValue getDamlState(final DamlStateKey key) throws InternalError {
    LOG.debug(String.format("Getting DAML state for key=[%s]", key));
    if (key.getKeyCase().equals(DamlStateKey.KeyCase.COMMAND_DEDUP)) {
      return DamlStateValue.newBuilder().setCommandDedup(DamlCommandDedupValue.newBuilder().build()).build();
    }

    final UInt256 address = Namespace.makeDamlStateKeyAddress(key);
    LOG.debug(String.format("DAML state key address=[%s]", address.toHexString()));
    final ByteBuffer buf = getLedgerEntry(address);
    if (buf == null) {
      LOG.debug(String.format("No ledger entry for DAML state key address=[%s]", address.toHexString()));
      return null;
    }
    try {
      return DamlStateValue.parseFrom(buf);
    } catch (final InvalidProtocolBufferException ipbe) {
      throw new InternalError("Failed to parse DAML state", ipbe);
    }
  }

  @Override
  public Map<DamlStateKey, DamlStateValue> getDamlStates(final Collection<DamlStateKey> keys) throws InternalError {
    final Map<DamlStateKey, DamlStateValue> states = new LinkedHashMap<>();
    keys.forEach(key -> {
      try {
        DamlStateValue val = getDamlState(key);
        if (val != null) {
          states.put(key, getDamlState(key));
        }
      } catch (final InternalError e) {
        LOG.error("Failed to parse DAML state:", e);
      }
    });
    return states;
  }

  @Override
  public Map<DamlStateKey, DamlStateValue> getDamlStates(final DamlStateKey... keys) throws InternalError {
    return getDamlStates(Lists.newArrayList(keys));
  }

  @Override
  public DamlLogEntry getDamlLogEntry(final DamlLogEntryId entryId) throws InternalError {
    LOG.debug(String.format("Getting DAML log entry for id=[%s]", entryId));
    final UInt256 address = Namespace.makeDamlLogEntryIdAddress(entryId);
    LOG.debug(String.format("DAML log entry id address=[%s]", address.toHexString()));
    final ByteBuffer buf = getLedgerEntry(address);
    if (buf == null) {
      LOG.debug(String.format("No ledger entry for DAML log entry id address=[%s]", address.toHexString()));
      return null;
    }
    try {
      return DamlLogEntry.parseFrom(buf);
    } catch (final InvalidProtocolBufferException ipbe) {
      throw new InternalError("Failed to parse daml log entry", ipbe);
    }
  }

  private ByteBuffer getLedgerEntry(final UInt256 key) {
    // reconstitute RLP bytes from all ethereum slices created for this ledger entry
    final Map<Bytes32, AccountStorageEntry> entryMap = account.storageEntriesFrom(key.toBytes(), Integer.MAX_VALUE);
    if (entryMap.isEmpty()) {
      return null;
    }

    Bytes rawRlp = Bytes.EMPTY;
    for (final AccountStorageEntry e : entryMap.values()) {
      rawRlp = Bytes.concatenate(rawRlp, e.getValue().toBytes());
    }

    final Bytes entry = RLP.decodeOne(rawRlp);
    if (!entry.isEmpty() || entry.isZero()) {
      return ByteBuffer.wrap(entry.toArray());
    } else {
      throw new InternalError("Cannot parse empty daml ledger entry");
    }
  }

  @Override
  public Map<DamlLogEntryId, DamlLogEntry> getDamlLogEntries(final Collection<DamlLogEntryId> ids)
      throws InternalError {
    final Map<DamlLogEntryId, DamlLogEntry> logs = new LinkedHashMap<>();
    ids.forEach(id -> {
      try {
        DamlLogEntry entry = getDamlLogEntry(id);
        if (entry != null) {
          logs.put(id, getDamlLogEntry(id));
        }
      } catch (final InternalError e) {
        LOG.error("Failed to parse daml log entry:", e);
      }
    });
    return logs;
  }

  @Override
  public Map<DamlLogEntryId, DamlLogEntry> getDamlLogEntries(final DamlLogEntryId... ids) throws InternalError {
    return getDamlLogEntries(Lists.newArrayList(ids));
  }

  /**
   * Add the supplied data to the ledger, starting at the supplied ethereum
   * storage slot address.
   *
   * @param rootAddress 256-bit ethereum storage slot address
   * @param entry       value to store in the ledger
   */
  private void addLedgerEntry(final UInt256 rootAddress, final ByteString entry) {
    // RLP-encode the entry
    final Bytes encoded = RLP.encodeOne(Bytes.of(entry.toByteArray()));

    // store the first part of the entry
    int sliceSz = Math.min(Namespace.STORAGE_SLOT_SIZE, encoded.size());
    Bytes data = encoded.slice(0, sliceSz);
    UInt256 slot = rootAddress;
    account.setStorageValue(slot, UInt256.fromBytes(data));

    // Store remaining parts, if any. We ensure that the data is stored in
    // consecutive
    // ethereum storage slots by incrementing the slot by one each time
    int offset = Namespace.STORAGE_SLOT_SIZE;
    while (offset < encoded.size()) {
      final int length = Math.min(Namespace.STORAGE_SLOT_SIZE, encoded.size() - offset);
      data = encoded.slice(offset, length);
      slot = slot.add(1);
      account.setStorageValue(slot, UInt256.fromBytes(data));

      offset += Namespace.STORAGE_SLOT_SIZE;
    }
  }

  @Override
  public void setDamlState(final DamlStateKey key, final DamlStateValue value) throws InternalError {
    final ByteString packedKey = KeyValueCommitting.packDamlStateKey(key);
    final ByteString packedValue = key.getKeyCase().equals(DamlStateKey.KeyCase.COMMAND_DEDUP) ? packedKey
        : KeyValueCommitting.packDamlStateValue(value);
    final UInt256 rootAddress = Namespace.makeAddress(Namespace.DamlKeyType.STATE, packedKey);
    addLedgerEntry(rootAddress, packedValue);
  }

  @Override
  public void setDamlStates(final Collection<Entry<DamlStateKey, DamlStateValue>> entries) throws InternalError {
    entries.forEach(e -> setDamlState(e.getKey(), e.getValue()));
  }

  @Override
  public UInt256 addDamlLogEntry(final DamlLogEntryId entryId, final DamlLogEntry entry) throws InternalError {
    final ByteString packedEntryId = KeyValueCommitting.packDamlLogEntryId(entryId);
    final UInt256 rootAddress = Namespace.makeAddress(Namespace.DamlKeyType.LOG, packedEntryId);
    addLedgerEntry(rootAddress, KeyValueCommitting.packDamlLogEntry(entry));
    return rootAddress;
  }

  @Override
  public void sendLogEvent(final DamlLogEntryId entryId, final DamlLogEntry entry) throws InternalError {
    throw new InternalError("Method not implemented");
  }

  @Override
  public Timestamp getRecordTime() throws InternalError {
    Instant time = Instant.now();
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
    return timestamp;
  }

  @Override
  public void updateLogEntryIndex(final List<String> addresses) throws InternalError {
    throw new InternalError("Method not implemented");
  }

  @Override
  public List<String> getLogEntryIndex() throws InternalError {
    throw new InternalError("Method not implemented");
  }

  @Override
  public TimeModel getTimeModel() throws InternalError {
    throw new InternalError("Method not implemented");
  }

  @Override
  public void setTimeModel(final TimeModel tm) throws InternalError {
    throw new InternalError("Method not implemented");
  }
}
