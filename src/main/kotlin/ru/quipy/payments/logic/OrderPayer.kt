package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    private val account by lazy { paymentService.executionProperties() }

    private val processingMillis by lazy {
        account.averageProcessingTime.toMillis().coerceAtLeast(1L)
    }

    private val throughputPerSecond by lazy {
        minOf(
            account.rateLimitPerSec.toDouble(),
            account.parallelRequests * 1_000.0 / processingMillis,
        ).coerceAtLeast(1.0)
    }

    private val maxConcurrentRequests by lazy {
        minOf(
            account.parallelRequests,
            ceil(throughputPerSecond * processingMillis / 1_000.0).toInt(),
        ).coerceAtLeast(1)
    }

    private val paymentsInProgress = AtomicInteger(0)

    private val paymentExecutor by lazy {
        ThreadPoolExecutor(
            maxConcurrentRequests,
            maxConcurrentRequests,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(8_000),
            NamedThreadFactory("payment-submission-executor"),
            CallerBlockingRejectedExecutionHandler()
        )
    }

    @PostConstruct
    fun registerExecutorMetrics() {
        Gauge.builder(
            "order_payer_queue_size",
            paymentExecutor.queue
        ) { queue -> queue.size.toDouble() }
            .description("Current number of payment tasks waiting in the queue")
            .baseUnit("tasks")
            .register(meterRegistry)

        Gauge.builder(
            "order_payer_active_workers",
            paymentExecutor
        ) { executor -> executor.activeCount.toDouble() }
            .description("Current number of active payment worker threads")
            .baseUnit("threads")
            .register(meterRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (!reservePlaceBefore(deadline)) {
            logger.warn("Payment $paymentId cannot be completed before deadline")
            return createdAt
        }

        paymentExecutor.submit {
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } finally {
                paymentsInProgress.decrementAndGet()
            }
        }
        return createdAt
    }

    private fun reservePlaceBefore(deadline: Long): Boolean {
        while (true) {
            val inProgress = paymentsInProgress.get()
            val waiting = maxOf(0, inProgress + 1 - maxConcurrentRequests)
            val waitMillis = ceil(waiting * 1_000.0 / throughputPerSecond).toLong()

            if (System.currentTimeMillis() + waitMillis + processingMillis > deadline) {
                return false
            }

            if (paymentsInProgress.compareAndSet(inProgress, inProgress + 1)) {
                return true
            }
        }
    }
}
