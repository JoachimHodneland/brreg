"""Rene funksjoner: rå API-JSON → flate dict-er klare for CSV."""
from datetime import date


def _kode(obj: dict) -> str:
    return (obj or {}).get("kode", "")


def _besk(obj: dict) -> str:
    return (obj or {}).get("beskrivelse", "")


def _adr(e: dict, key: str) -> dict:
    return e.get(key) or {}


# ── Enheter / Underenheter ────────────────────────────────────────────────────

def enhet_to_row(e: dict) -> dict:
    fadr = _adr(e, "forretningsadresse")
    padr = _adr(e, "postadresse")
    nk1  = e.get("naeringskode1") or {}
    nk2  = e.get("naeringskode2") or {}
    nk3  = e.get("naeringskode3") or {}
    kap  = e.get("kapital") or {}
    sek  = e.get("institusjonellSektorkode") or {}
    return {
        "organisasjonsnummer":              e.get("organisasjonsnummer", ""),
        "navn":                             e.get("navn", ""),
        "naeringskode1":                    _kode(nk1),
        "naeringskode1_beskrivelse":        _besk(nk1),
        "naeringskode2":                    _kode(nk2),
        "naeringskode2_beskrivelse":        _besk(nk2),
        "naeringskode3":                    _kode(nk3),
        "naeringskode3_beskrivelse":        _besk(nk3),
        "organisasjonsform":                _kode(e.get("organisasjonsform") or {}),
        "forretningsadresse":               "; ".join(fadr.get("adresse") or []),
        "forretningsadresse_postnummer":    fadr.get("postnummer", ""),
        "forretningsadresse_poststed":      fadr.get("poststed", ""),
        "forretningsadresse_kommune":       fadr.get("kommune", ""),
        "forretningsadresse_kommunenummer": fadr.get("kommunenummer", ""),
        "postadresse_postnummer":           padr.get("postnummer", ""),
        "postadresse_poststed":             padr.get("poststed", ""),
        "antall_ansatte":                   e.get("antallAnsatte", ""),
        "stiftelsesdato":                   e.get("stiftelsesdato", ""),
        "registreringsdato":                e.get("registreringsdatoEnhetsregisteret", ""),
        "hjemmeside":                       e.get("hjemmeside", ""),
        "telefon":                          e.get("telefon", ""),
        "mobil":                            e.get("mobil", ""),
        "epostadresse":                     e.get("epostadresse", ""),
        "er_i_konsern":                     e.get("erIKonsern", ""),
        "overordnet_enhet":                 e.get("overordnetEnhet", ""),
        "kapital_belop":                    kap.get("belop", ""),
        "kapital_type":                     kap.get("type", ""),
        "kapital_valuta":                   kap.get("valuta", ""),
        "siste_innsendte_aarsregnskap":     e.get("sisteInnsendteAarsregnskap", ""),
        "institusjonell_sektor":            _kode(sek),
        "institusjonell_sektor_beskrivelse": _besk(sek),
        "registrert_i_mva":                 e.get("registrertIMvaregisteret", ""),
        "registrert_i_foretaksregisteret":  e.get("registrertIForetaksregisteret", ""),
        "konkurs":                          e.get("konkurs", ""),
        "under_avvikling":                  e.get("underAvvikling", ""),
        "under_tvangsavvikling":            e.get("underTvangsavviklingEllerTvangsopplosning", ""),
        "antall_underenheter":              "",   # beregnes i finalize-fase
        "hentet_dato":                      str(date.today()),
    }


def underenhet_to_row(e: dict) -> dict:
    """Underenhet er som enhet men med oppstarts-/nedleggelsesdato."""
    row = enhet_to_row(e)
    row["overordnet_enhet"] = e.get("overordnetEnhet", "")
    row["oppstartsdato"]    = e.get("oppstartsdato", "")
    row["nedleggelsesdato"] = e.get("nedleggelsesdato", "")
    return row


# ── Roller ────────────────────────────────────────────────────────────────────

# Rolle-typer som indikerer eierskap (relevant for ikke-AS-selskaper)
EIERSKAP_ROLLER = {"INNH", "DTSO", "DTPR", "KOMP", "BEST"}

# Rolle-typer nyttige for konsernanalyse
KONSERN_ROLLER = {"LEDE", "DAGL", "REVI", "REGN"} | EIERSKAP_ROLLER


def roller_to_rows(orgnr: str, rollegrupper: list) -> list:
    """Flat ut rollegrupper for ett selskap til liste av CSV-rader."""
    rows = []
    for gruppe in (rollegrupper or []):
        grp_type = gruppe.get("type") or {}
        grp_kode = _kode(grp_type)
        grp_besk = _besk(grp_type)
        grp_dato = gruppe.get("sistEndret", "")
        for rolle in (gruppe.get("roller") or []):
            rtype      = rolle.get("type") or {}
            person     = rolle.get("person") or {}
            enhet_obj  = rolle.get("enhet") or {}
            p_navn     = person.get("navn") or {}
            rows.append({
                "orgnr_selskap":            orgnr,
                "rollegruppe_kode":         grp_kode,
                "rollegruppe_beskrivelse":  grp_besk,
                "rollegruppe_sist_endret":  grp_dato,
                "rolle_type_kode":          _kode(rtype),
                "rolle_type_beskrivelse":   _besk(rtype),
                "person_type":              "enhet" if enhet_obj else "person",
                "fodselsdato":              person.get("fodselsdato", ""),
                "fornavn":                  p_navn.get("fornavn", ""),
                "etternavn":                p_navn.get("etternavn", ""),
                "er_doed":                  person.get("erDoed", ""),
                "enhet_orgnr":              enhet_obj.get("organisasjonsnummer", ""),
                "enhet_navn":               "; ".join(enhet_obj.get("navn") or []),
                "enhet_orgform":            _kode(enhet_obj.get("organisasjonsform") or {}),
                "fratraadt":                rolle.get("fratraadt", ""),
                "avregistrert":             rolle.get("avregistrert", ""),
                "rekkefolge":               rolle.get("rekkefolge", ""),
                "hentet_dato":              str(date.today()),
            })
    return rows


def is_eierskap(row: dict) -> bool:
    return row.get("rolle_type_kode") in EIERSKAP_ROLLER


# ── Regnskap ──────────────────────────────────────────────────────────────────

def regnskap_to_row(r: dict, enhet_row: dict = None) -> dict:
    e        = enhet_row or {}
    virksom  = r.get("virksomhet") or {}
    periode  = r.get("regnskapsperiode") or {}
    res      = r.get("resultatregnskapResultat") or {}
    drift    = res.get("driftsresultat") or {}
    innt     = drift.get("driftsinntekter") or {}
    kost     = drift.get("driftskostnad") or {}
    finans   = res.get("finansresultat") or {}
    fin_i    = finans.get("finansinntekt") or {}
    fin_k    = finans.get("finanskostnad") or {}
    ekg      = r.get("egenkapitalGjeld") or {}
    ek       = ekg.get("egenkapital") or {}
    gjeld_o  = ekg.get("gjeldOversikt") or {}
    eiend    = r.get("eiendeler") or {}
    til      = periode.get("tilDato", "")
    return {
        "organisasjonsnummer":           e.get("organisasjonsnummer") or virksom.get("organisasjonsnummer", ""),
        "navn":                          e.get("navn", ""),
        "naeringskode":                  e.get("naeringskode1", ""),
        "organisasjonsform":             e.get("organisasjonsform", ""),
        "kommune":                       e.get("forretningsadresse_kommune", ""),
        "postnummer":                    e.get("forretningsadresse_postnummer", ""),
        "poststed":                      e.get("forretningsadresse_poststed", ""),
        "antall_ansatte":                e.get("antall_ansatte", ""),
        "overordnet_enhet":              e.get("overordnet_enhet", ""),
        "er_i_konsern":                  e.get("er_i_konsern", ""),
        "regnskapsaar":                  til[:4] if til else "",
        "fra_dato":                      periode.get("fraDato", ""),
        "til_dato":                      til,
        "regnskapstype":                 r.get("regnskapstype", ""),
        "oppstillingsplan":              r.get("oppstillingsplan", ""),
        "sum_driftsinntekter":           innt.get("sumDriftsinntekter", ""),
        "sum_driftskostnad":             kost.get("sumDriftskostnad", ""),
        "driftsresultat":                drift.get("driftsresultat", ""),
        "sum_finansinntekter":           fin_i.get("sumFinansinntekter", ""),
        "sum_finanskostnad":             fin_k.get("sumFinanskostnad", ""),
        "ordinaert_resultat_foer_skatt": res.get("ordinaertResultatFoerSkattekostnad", ""),
        "aarsresultat":                  res.get("aarsresultat", ""),
        "sum_eiendeler":                 eiend.get("sumEiendeler", ""),
        "sum_anleggsmidler":             (eiend.get("anleggsmidler") or {}).get("sumAnleggsmidler", ""),
        "sum_omloepsmidler":             (eiend.get("omloepsmidler") or {}).get("sumOmloepsmidler", ""),
        "sum_egenkapital":               ek.get("sumEgenkapital", ""),
        "sum_gjeld":                     gjeld_o.get("sumGjeld", ""),
        "sum_kortsiktig_gjeld":          (gjeld_o.get("kortsiktigGjeld") or {}).get("sumKortsiktigGjeld", ""),
        "sum_langsiktig_gjeld":          (gjeld_o.get("langsiktigGjeld") or {}).get("sumLangsiktigGjeld", ""),
        "pdf_aar_tilgjengelig":          "",   # fylles inn av regnskap.py
        "hentet_dato":                   str(date.today()),
    }
