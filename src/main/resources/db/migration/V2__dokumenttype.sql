CREATE TYPE dokument_type AS ENUM ('Inntektsmelding', 'Søknad');

ALTER TABLE oppgave_tilstand ADD dokument_type dokument_type NOT NULL;
