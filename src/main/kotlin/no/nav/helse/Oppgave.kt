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

    fun håndter(nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.DokumentOppdaget) =
        tilstand.håndter(this, nyTilstand)

    fun håndter(nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.LagOppgave) = tilstand.håndter(this, nyTilstand)
    fun håndter(nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.Avsluttet) = tilstand.håndter(this, nyTilstand)
    fun håndter(nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.Lest) = tilstand.håndter(this, nyTilstand)

    private fun tilstand(tilstand: Tilstand) {
        this.tilstand = tilstand
        observer?.lagre(this)
        tilstand.entring(this)
    }

    sealed class Tilstand {
        open fun entring(oppgave: Oppgave) {
            oppgave.observer?.publiser(oppgave)
        }

        open fun håndter(oppgave: Oppgave, nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.DokumentOppdaget) {}
        open fun håndter(oppgave: Oppgave, nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.LagOppgave) {}
        open fun håndter(oppgave: Oppgave, nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.Avsluttet) {}
        open fun håndter(oppgave: Oppgave, nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.Lest) {}

        object SpleisFerdigbehandlet : Tilstand()
        object LagOppgave : Tilstand()

        object SpleisLest : Tilstand() {
            override fun håndter(oppgave: Oppgave, nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.LagOppgave) {
                oppgave.tilstand(LagOppgave)
            }

            override fun håndter(oppgave: Oppgave, nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.Avsluttet) {
                oppgave.tilstand(SpleisFerdigbehandlet)
            }
        }

        object DokumentOppdaget : Tilstand() {
            override fun entring(oppgave: Oppgave) {}
            override fun håndter(oppgave: Oppgave, nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.LagOppgave) {
                oppgave.tilstand(LagOppgave)
            }

            override fun håndter(oppgave: Oppgave, nyTilstand: HåndterVedtaksperiodeendringer.Tilstand.Lest) {
                oppgave.tilstand(SpleisLest)
            }
        }
    }
}

enum class DokumentType {
    Inntektsmelding, Søknad
}
