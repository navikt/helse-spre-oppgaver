package no.nav.helse

enum class DatabaseTilstand(val gyldigeOverganger: Set<DatabaseTilstand>) {
    SpleisFerdigbehandlet(setOf()),
    LagOppgave(setOf()),
    SpleisLest(setOf(SpleisFerdigbehandlet, LagOppgave)),
    DokumentOppdaget(setOf(SpleisLest, LagOppgave));

    fun godtarOvergang(databaseTilstand: DatabaseTilstand) = gyldigeOverganger.contains(databaseTilstand)
}
