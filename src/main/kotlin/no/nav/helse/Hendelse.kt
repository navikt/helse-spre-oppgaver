package no.nav.helse

sealed class Hendelse {
        abstract fun accept(oppgave: Oppgave)

        object TilInfotrygd : Hendelse() {
            override fun accept(oppgave: Oppgave) {
                oppgave.håndter(this)
            }
        }

        object Avsluttet : Hendelse() {
            override fun accept(oppgave: Oppgave) {
                oppgave.håndter(this)
            }
        }

        object AvsluttetUtenUtbetalingMedInntektsmelding : Hendelse() {
            override fun accept(oppgave: Oppgave) {
                oppgave.håndter(this)
            }
        }


        object Lest : Hendelse() {
            override fun accept(oppgave: Oppgave) {
                oppgave.håndter(this)
            }
        }
    }
