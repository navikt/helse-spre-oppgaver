CREATE TYPE tilstand_type AS ENUM ('DokumentOppdaget', 'SpleisLest', 'SpleisFerdigbehandlet', 'LagOppgave');

CREATE TABLE oppgave_tilstand(hendelse_id UUID NOT NULL PRIMARY KEY, dokument_id UUID, tilstand tilstand_type NOT NULL DEFAULT 'DokumentOppdaget')
