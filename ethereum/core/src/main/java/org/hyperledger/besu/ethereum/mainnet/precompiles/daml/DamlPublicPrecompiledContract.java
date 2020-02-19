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
import org.hyperledger.besu.ethereum.vm.GasCalculator;
import org.hyperledger.besu.ethereum.vm.MessageFrame;

import org.apache.tuweni.bytes.Bytes;

public class DamlPublicPrecompiledContract extends AbstractPrecompiledContract {
  private static final String DAML_PUBLIC = "DamlPublic";

  public DamlPublicPrecompiledContract(final GasCalculator gasCalculator) {
    super(DAML_PUBLIC, gasCalculator);
  }

  @Override
  public Gas gasRequirement(final Bytes input) {
    return Gas.ZERO;
  }

  @Override
  @SuppressWarnings("unused")
  public Bytes compute(final Bytes input, final MessageFrame messageFrame) {
    final Address from = messageFrame.getSenderAddress();
    final Address to = messageFrame.getRecipientAddress();
    final long timestamp = messageFrame.getBlockHeader().getTimestamp();
    final WorldUpdater updater = messageFrame.getWorldState();
    final MutableAccount account =
        messageFrame.getWorldState().getAccount(messageFrame.getSenderAddress()).getMutable();
    final LedgerState state =
        new DamlLedgerState(
            messageFrame.getSenderAddress(),
            messageFrame.getRecipientAddress(),
            messageFrame.getBlockHeader().getTimestamp());

    updater.commit();

    return Bytes.EMPTY; // changes world state
  }
}
