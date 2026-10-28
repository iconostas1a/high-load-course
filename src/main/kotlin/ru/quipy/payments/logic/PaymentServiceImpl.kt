package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import java.util.*

class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val accountProperties: List<PaymentAccountProperties>,
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    override fun executionProperties(): PaymentAccountProperties {
        return accountProperties.single()
    }
}