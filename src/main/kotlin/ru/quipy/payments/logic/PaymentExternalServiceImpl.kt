package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        private const val MAX_ATTEMPTS = 3
        private const val RETRY_DELAY_MILLIS = 100L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimiter = SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))
    private val parallelLimiter = Semaphore(parallelRequests, true)
    private val client = OkHttpClient.Builder().build()
    private val processingMillis = requestAverageProcessingTime.toMillis().coerceAtLeast(1L)

    private fun remainingWaitMillis(deadline: Long): Long = deadline - now() - processingMillis

    private data class AttemptResult(
        val success: Boolean,
        val retryable: Boolean,
        val reason: String?,
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        var submitted = false
        var result: AttemptResult? = null
        var failureReason = "payment deadline exceeded before sending"

        for (attempt in 1..MAX_ATTEMPTS) {
            val waitForParallel = remainingWaitMillis(deadline)
            if (waitForParallel <= 0) break

            val parallelPermitReceived = try {
                parallelLimiter.tryAcquire(waitForParallel, TimeUnit.MILLISECONDS)
            } catch (exception: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
            if (!parallelPermitReceived) {
                failureReason = "no parallel request permission received before deadline"
                break
            }

            try {
                val waitForRate = remainingWaitMillis(deadline)
                if (waitForRate <= 0 || !rateLimiter.tickBlocking(Duration.ofMillis(waitForRate))) {
                    failureReason = "no rate limit permission received before deadline"
                    break
                }

                if (!submitted) {
                    paymentESService.update(paymentId) {
                        it.logSubmission(
                            success = true,
                            transactionId,
                            now(),
                            Duration.ofMillis(now() - paymentStartedAt)
                        )
                    }
                    submitted = true
                }

                result = sendPaymentRequest(paymentId, amount, transactionId, deadline)
            } finally {
                parallelLimiter.release()
            }

            if (result.success || !result.retryable || attempt == MAX_ATTEMPTS) break
            if (now() + RETRY_DELAY_MILLIS + processingMillis >= deadline) break

            try {
                Thread.sleep(RETRY_DELAY_MILLIS)
            } catch (exception: InterruptedException) {
                Thread.currentThread().interrupt()
                break
            }
        }

        if (!submitted) {
            markNotSent(paymentId, failureReason)
            return
        }

        val finalResult = result ?: AttemptResult(false, false, failureReason)
        paymentESService.update(paymentId) {
            it.logProcessing(finalResult.success, now(), transactionId, reason = finalResult.reason)
        }
        meterRegistry.counter(
            "online_shop_payments",
            "stage", "completed",
            "account", accountName,
            "outcome", if (finalResult.success) "success" else "failed"
        ).increment()
    }

    private fun sendPaymentRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): AttemptResult {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        return try {
            meterRegistry.counter(
                "online_shop_payments",
                "stage", "outgoing",
                "account", accountName,
                "outcome", "none"
            ).increment()

            val call = client.newCall(request)
            call.timeout().timeout((deadline - now()).coerceAtLeast(1L), TimeUnit.MILLISECONDS)
            call.execute().use { response ->
                if (response.code == 408 || response.code == 429 || response.code >= 500) {
                    return AttemptResult(false, true, "HTTP ${response.code}")
                }

                val responseBody = response.body?.string()
                val body = try {
                    mapper.readValue(responseBody, ExternalSysResponse::class.java)
                } catch (exception: Exception) {
                    logger.error("[$accountName] Invalid response for payment $paymentId: HTTP ${response.code}", exception)
                    return AttemptResult(false, false, "Invalid provider response")
                }

                val success = response.isSuccessful && body.result
                AttemptResult(success, false, body.message)
            }
        } catch (exception: SocketTimeoutException) {
            logger.error("[$accountName] Payment timeout for $paymentId", exception)
            AttemptResult(false, true, "Request timeout")
        } catch (exception: IOException) {
            logger.error("[$accountName] payment request failed for $paymentId", exception)
            AttemptResult(false, true, exception.message)
        } catch (exception: Exception) {
            logger.error("[$accountName] payment failed for $paymentId", exception)
            AttemptResult(false, false, exception.message)
        }
    }

    private fun markNotSent(paymentId: UUID, reason: String) {
        logger.warn("[$accountName] payment $paymentId was not sent: $reason")
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), reason = reason)
        }
        meterRegistry.counter(
            "online_shop_payments",
            "stage", "completed",
            "account", accountName,
            "outcome", "timeout"
        ).increment()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
