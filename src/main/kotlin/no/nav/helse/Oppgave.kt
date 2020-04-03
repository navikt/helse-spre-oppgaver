package no.nav.helse

import java.util.*

class Oppgave(
    val hendelseId: UUID,
    val dokumentId: UUID,
    var tilstand: Tilstand = Tilstand.DokumentOppdaget,
    val dokumentType: DokumentType
) {
    private var observer: Observer? = null
    fun setObserver(observer: Observer) = apply {
        this.observer = observer
    }

    interface Observer {
        fun lagre(oppgave: Oppgave) {}
        fun publiser(oppgave: Oppgave) {}
    }

    fun håndter(hendelse: HåndterVedtaksperiodeendringer.Hendelse.TilInfotrygd) = tilstand.håndter(this, hendelse)
    fun håndter(hendelse: HåndterVedtaksperiodeendringer.Hendelse.Avsluttet) = tilstand.håndter(this, hendelse)
    fun håndter(hendelse: HåndterVedtaksperiodeendringer.Hendelse.Lest) = tilstand.håndter(this, hendelse)

    private fun tilstand(tilstand: Tilstand) {
        this.tilstand = tilstand
        observer?.lagre(this)
        tilstand.entring(this)
    }

    sealed class Tilstand {
        open fun entring(oppgave: Oppgave) {
            oppgave.observer?.publiser(oppgave)
        }

        open fun håndter(oppgave: Oppgave, hendelse: HåndterVedtaksperiodeendringer.Hendelse.TilInfotrygd) {}
        open fun håndter(oppgave: Oppgave, hendelse: HåndterVedtaksperiodeendringer.Hendelse.Avsluttet) {}
        open fun håndter(oppgave: Oppgave, hendelse: HåndterVedtaksperiodeendringer.Hendelse.Lest) {}

        object SpleisFerdigbehandlet : Tilstand()
        object LagOppgave : Tilstand()

        object SpleisLest : Tilstand() {
            override fun håndter(oppgave: Oppgave, hendelse: HåndterVedtaksperiodeendringer.Hendelse.TilInfotrygd) {
                oppgave.tilstand(LagOppgave)
            }

            override fun håndter(oppgave: Oppgave, hendelse: HåndterVedtaksperiodeendringer.Hendelse.Avsluttet) {
                oppgave.tilstand(SpleisFerdigbehandlet)
            }
        }

        object DokumentOppdaget : Tilstand() {
            override fun entring(oppgave: Oppgave) {}
            override fun håndter(oppgave: Oppgave, hendelse: HåndterVedtaksperiodeendringer.Hendelse.TilInfotrygd) {
                oppgave.tilstand(LagOppgave)
            }

            override fun håndter(oppgave: Oppgave, hendelse: HåndterVedtaksperiodeendringer.Hendelse.Lest) {
                oppgave.tilstand(SpleisLest)
            }
        }

        override fun toString(): String {
            return this.javaClass.simpleName
        }
    }
}

enum class DokumentType {
    Inntektsmelding, Søknad
}
