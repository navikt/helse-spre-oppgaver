package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.convertValue
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.assertEquals

class AppTest {

    @Test
    fun test() = runBlocking {
        val fnr = "12345678910"
        val orgnr = "987654321"
        val opprettet = LocalDateTime.now()
        val json = asJsonNode(
            mapOf(
                "@event_name" to "trenger_inntektsmelding",
                "vedtaksperiodeId" to UUID.randomUUID(),
                "fødselsnummer" to fnr,
                "organisasjonsnummer" to orgnr,
                "opprettet" to opprettet,
                "fom" to LocalDate.now(),
                "tom" to LocalDate.now()
            )
        )
        val expected = listOf(
            TrengerInntektsmeldingDTO(
                organisasjonsnummer = orgnr,
                fødselsnummer = fnr,
                fom = LocalDate.now(),
                tom = LocalDate.now(),
                opprettet = opprettet
            ),
            TrengerInntektsmeldingDTO(
                organisasjonsnummer = orgnr,
                fødselsnummer = fnr,
                fom = LocalDate.now(),
                tom = LocalDate.now(),
                opprettet = opprettet
            )
        )
        val actual = listOf(
            ("123".toByteArray() to json),
            ("234".toByteArray() to null),
            ("345".toByteArray() to json)
        ).asFlow().inntektsmeldingFlow().toList()
        assertEquals(expected, actual)
    }

    private fun asJsonNode(values: Map<String, Any>) = objectMapper.convertValue<JsonNode>(values)

}
