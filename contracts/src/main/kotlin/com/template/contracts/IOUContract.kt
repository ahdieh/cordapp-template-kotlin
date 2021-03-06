package com.template.contracts;

import com.template.states.IOUState;
import net.corda.core.contracts.*;
import net.corda.core.transactions.LedgerTransaction;

class IOUContract : Contract {
    companion object {
        // Used to identify our contract when building a transaction.
        const val ID = "com.template.contracts.IOUContract"
    }

    // A transaction is valid if the verify() function of the contract of all the transaction's input and output states
    // does not throw an exception.
    override fun verify(tx: LedgerTransaction) {
        // Verification logic goes here.
    }

    // Used to indicate the transaction's intent.

    interface Commands : CommandData {
        class Action : Commands
    }
}