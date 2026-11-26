package ru.quipy.payments.logic

import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

@Component
class PaymentAttemptDurationStore {
    companion object {
        private const val MAX_SAMPLES_PER_ACCOUNT = 10_000
    }

    data class Sample(val durationNanos: Long, val outcome: String)

    private val samplesByAccount = ConcurrentHashMap<String, MutableList<Sample>>()

    fun record(account: String, durationNanos: Long, outcome: String) {
        val samples = samplesByAccount.computeIfAbsent(account) { mutableListOf() }
        synchronized(samples) {
            if (samples.size < MAX_SAMPLES_PER_ACCOUNT) {
                samples.add(Sample(durationNanos, outcome))
            }
        }
    }

    fun durations(account: String, outcome: String?): List<Double> {
        val samples = samplesByAccount[account] ?: return emptyList()
        return synchronized(samples) {
            samples.asSequence()
                .filter { outcome == null || it.outcome == outcome }
                .map { it.durationNanos / 1_000_000.0 }
                .toList()
        }
    }
}

@RestController
class PaymentAttemptDurationController(private val store: PaymentAttemptDurationStore) {
    @GetMapping("/debug/payment-attempt-durations", produces = [MediaType.TEXT_PLAIN_VALUE])
    fun durations(
        @RequestParam account: String,
        @RequestParam(required = false) outcome: String?,
    ): String {
        val values = store.durations(account, outcome)
        return if (values.isEmpty()) {
            "No payment attempts recorded for account=$account outcome=${outcome ?: "all"}"
        } else {
            "count=${values.size}; duration_ms=" +
                values.sorted().joinToString(", ") { String.format(Locale.ROOT, "%.3f", it) }
        }
    }
}
