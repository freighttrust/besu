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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateValue;
import com.daml.ledger.participant.state.v1.TimeModel;
import com.google.protobuf.Timestamp;

@SuppressWarnings("unused")
public class DamlLedgerState implements LedgerState {
  private final Address from;
  private final Address to;
  private final long timestamp;

  public DamlLedgerState(final Address from, final Address to, final long timestamp) {
    this.from = from;
    this.to = to;
    this.timestamp = timestamp;
  }

  @Override
  public DamlStateValue getDamlState(final DamlStateKey key) throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<DamlStateKey, DamlStateValue> getDamlStates(final Collection<DamlStateKey> keys)
      throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<DamlStateKey, DamlStateValue> getDamlStates(final DamlStateKey... keys)
      throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<DamlLogEntryId, DamlLogEntry> getDamlLogEntries(final Collection<DamlLogEntryId> keys)
      throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<DamlLogEntryId, DamlLogEntry> getDamlLogEntries(final DamlLogEntryId... keys)
      throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DamlLogEntry getDamlLogEntry(final DamlLogEntryId entryId) throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setDamlState(final DamlStateKey key, final DamlStateValue value)
      throws InternalError {
    // TODO Auto-generated method stub
  }

  @Override
  public void setDamlStates(final Collection<Entry<DamlStateKey, DamlStateValue>> entries)
      throws InternalError {
    // TODO Auto-generated method stub
  }

  @Override
  public List<String> addDamlLogEntry(final DamlLogEntryId entryId, final DamlLogEntry entry)
      throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void sendLogEvent(final DamlLogEntryId entryId, final DamlLogEntry entry)
      throws InternalError {
    // TODO Auto-generated method stub
  }

  @Override
  public Timestamp getRecordTime() throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateLogEntryIndex(final List<String> addresses) throws InternalError {
    // TODO Auto-generated method stub
  }

  @Override
  public List<String> getLogEntryIndex() throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeModel getTimeModel() throws InternalError {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setTimeModel(final TimeModel tm) throws InternalError {
    // TODO Auto-generated method stub
  }
}
