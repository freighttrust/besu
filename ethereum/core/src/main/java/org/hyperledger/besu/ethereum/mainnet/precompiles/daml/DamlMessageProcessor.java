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
import org.hyperledger.besu.ethereum.mainnet.AbstractMessageProcessor;
import org.hyperledger.besu.ethereum.vm.EVM;
import org.hyperledger.besu.ethereum.vm.MessageFrame;

import java.util.Collection;

/**
 * Class not used. Just here temporarily to show what {@link AbstractMessageProcessor} does with
 * incoming messages.
 */
public class DamlMessageProcessor extends AbstractMessageProcessor {
  public DamlMessageProcessor(
      final EVM evm, final Collection<Address> forceDeleteAccountsWhenEmpty) {
    super(evm, forceDeleteAccountsWhenEmpty);
  }

  @Override
  protected void start(final MessageFrame frame) {
    // TODO Auto-generated method stub
  }

  @Override
  protected void codeSuccess(final MessageFrame frame) {
    // TODO Auto-generated method stub
  }
}
