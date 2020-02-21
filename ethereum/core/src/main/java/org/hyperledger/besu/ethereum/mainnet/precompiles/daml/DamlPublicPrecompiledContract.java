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
import org.hyperledger.besu.ethereum.core.Gas;
import org.hyperledger.besu.ethereum.core.MutableAccount;
import org.hyperledger.besu.ethereum.core.WorldUpdater;
import org.hyperledger.besu.ethereum.mainnet.AbstractPrecompiledContract;
import org.hyperledger.besu.ethereum.mainnet.precompiles.daml.protobuf.DamlOperation;
import org.hyperledger.besu.ethereum.mainnet.precompiles.daml.protobuf.DamlParty;
import org.hyperledger.besu.ethereum.mainnet.precompiles.daml.protobuf.DamlTransaction;
import org.hyperledger.besu.ethereum.vm.GasCalculator;
import org.hyperledger.besu.ethereum.vm.MessageFrame;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateValue;
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission;
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting;
import com.daml.ledger.participant.state.kvutils.KeyValueSubmission;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;
import scala.Option;

public class DamlPublicPrecompiledContract extends AbstractPrecompiledContract {
  private static final Logger LOG = LogManager.getLogger();

  private static final String DAML_PUBLIC = "DamlPublic";

  public DamlPublicPrecompiledContract(final GasCalculator gasCalculator) {
    super(DAML_PUBLIC, gasCalculator);
  }

  @Override
  public Gas gasRequirement(final Bytes input) {
    LOG.trace(
        String.format(
            "In gasRequirement(input=%s) %s",
            input.toHexString(), stackTrace(Thread.currentThread().getStackTrace())));
    return Gas.ZERO;
  }

  @Override
  public Bytes compute(final Bytes input, final MessageFrame messageFrame) {
    final WorldUpdater updater = messageFrame.getWorldState();
    final MutableAccount account = updater.getAccount(Address.DAML_PUBLIC).getMutable();
    final LedgerState ledgerState = new DamlLedgerState(account);
    LOG.trace(
        String.format(
            "In compute(input=%s, target-account=%s) %s",
            input.toHexString(), account, stackTrace(Thread.currentThread().getStackTrace())));
    try {
      DamlOperation operation;
      Bytes payload;
      if (input.isEmpty()) {
        LOG.debug("Input is empty, faking something ...");
        DamlOperation.Builder builder = DamlOperation.newBuilder();
        DamlParty party = DamlParty.newBuilder().setHint("1").setDisplayName("camembert").build();
        operation = builder.setAllocateParty(party).build();
        payload = Bytes.of(operation.toByteArray());
      } else {
        operation = DamlOperation.parseFrom(input.toArray());
        payload = input;
      }
      LOG.debug(
          String.format(
              "Parsed DamlOperation protobuf %s [%s] from input [%s]",
              JsonFormat.printer().print(operation),
              Bytes.of(operation.toByteArray()).toHexString(),
              payload.toHexString()));
      if (operation.hasTransaction()) {
        DamlTransaction tx = operation.getTransaction();
        String participantId = operation.getSubmittingParticipant();
        DamlLogEntryId entryId = KeyValueCommitting.unpackDamlLogEntryId(tx.getLogEntryId());
        DamlSubmission submission = KeyValueSubmission.unpackDamlSubmission(tx.getSubmission());
        processTransaction(ledgerState, submission, participantId, entryId);
      }
    } catch (InvalidTransactionException e) {
      // exception called and consumed
    } catch (final InvalidProtocolBufferException ipbe) {
      Exception e =
          new RuntimeException(
              String.format(
                  "Payload is unparseable, and not a valid DamlSubmission %s",
                  ipbe.getMessage().getBytes(Charset.defaultCharset())),
              ipbe);
      LOG.error("Failed to parse DamlSubmission protocol buffer:", e);
    }

    return Bytes.EMPTY;
  }

  @SuppressWarnings("unused")
  private void processTransaction(
      final LedgerState ledgerState,
      final DamlSubmission submission,
      final String participantId,
      final DamlLogEntryId entryId)
      throws InternalError, InvalidTransactionException {
    long fetchStateStart = System.currentTimeMillis();
    Map<DamlStateKey, Option<DamlStateValue>> stateMap;
    //     = buildStateMap(ledgerState, txHeader, submission);

    // long recordStateStart = System.currentTimeMillis();
    // recordState(ledgerState, submission, participantId, stateMap, entryId);

    // long processFinished = System.currentTimeMillis();
    // long recordStateTime = processFinished - recordStateStart;
    // long fetchStateTime = recordStateStart - fetchStateStart;
    // LOG.info(String.format("Finished processing transaction %s times=[fetch=%s,record=%s]",
    //     txHeader.get.getPayloadSha512(), fetchStateTime, recordStateTime));
  }

  private static String stackTrace(final StackTraceElement[] stes) {
    final String PREFIX = "\n    at ";
    return String.format(
        "%s%s",
        PREFIX,
        Arrays.stream(stes)
            .skip(1)
            .map(StackTraceElement::toString)
            .collect(Collectors.joining(PREFIX)));
  }
}
