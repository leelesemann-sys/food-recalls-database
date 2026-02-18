# Food Recalls Database

> **Sprache:** [English](README.md) | Deutsch

Eine multinationale Plattform fuer Lebensmittelsicherheitsanalytik, die Rueckrufdaten von sechs Behoerden aus den USA, der EU und Grossbritannien in ein einheitliches Star-Schema-Data-Warehouse integriert. Aufgebaut auf Azure (Data Lake Gen2, Synapse Analytics, Data Factory) mit einer Python-ETL-Pipeline und Power BI Reporting-Schicht.

## Architektur

```
                      ┌──────────────┐
                      │  FDA API     │──┐
                      │  (US Food)   │  │
                      └──────────────┘  │
                      ┌──────────────┐  │    ┌──────────────────┐    ┌──────────────────┐
                      │  FSIS API    │──┤    │  Azure Data      │    │  ADLS Gen2       │
                      │  (US Meat)   │  ├───▶│  Factory         │───▶│  raw/ (Bronze)   │
                      └──────────────┘  │    │  PL_Ingest_*     │    │  JSON files      │
                      ┌──────────────┐  │    └──────────────────┘    └────────┬─────────┘
                      │  CDC NORS    │──┘                                     │
                      │  (Outbreaks) │                                        ▼
                      └──────────────┘               ┌───────────────────────────────────┐
                                                     │  Python ETL Pipeline              │
                      ┌──────────────┐               │  transform_to_star_schema.py      │
                      │  RASFF       │──────────────▶│  + 6 supporting scripts           │
                      │  (EU Alerts) │  CSV/Excel    │  (3,400+ lines)                   │
                      └──────────────┘  downloads    └────────────────┬──────────────────┘
                      ┌──────────────┐                                │
                      │  UK FSA      │───────────────────────────────▶│
                      │  (UK Alerts) │  JSON                         │
                      └──────────────┘                                ▼
                                                     ┌──────────────────────────────────┐
                      ┌──────────────┐               │  ADLS Gen2 gold/ (Gold)          │
                      │  FDA CAERS   │──────────────▶│  10 Parquet files (Star Schema)  │
                      │  (Adverse    │  CSV          │  5 Dimensions + 5 Facts          │
                      │   Events)    │               └────────────────┬─────────────────┘
                      └──────────────┘                                │
                                                                      ▼
                                                     ┌──────────────────────────────────┐
                                                     │  Azure Synapse Analytics         │
                                                     │  Serverless SQL Pool             │
                                                     │  External Tables + Views         │
                                                     └────────────────┬─────────────────┘
                                                                      │
                                                                      ▼
                                                     ┌──────────────────────────────────┐
                                                     │  Power BI Dashboard              │
                                                     │  DirectQuery on Synapse          │
                                                     └──────────────────────────────────┘
```

## Datenquellen

| Quelle | Behoerde | Region | Datensaetze | Zeitraum |
|--------|----------|--------|--------:|----------|
| **FDA Enforcement** | U.S. Food & Drug Administration | USA | ~28.000 | 2012 -- 2026 |
| **FSIS** | USDA Food Safety Inspection Service | USA | ~1.000 | 2012 -- 2024 |
| **RASFF** | Rapid Alert System for Food and Feed | EU | ~56.000 | 2002 -- 2026 |
| **UK FSA** | Food Standards Agency | UK | ~1.000 | 2019 -- 2026 |
| **CDC NORS** | National Outbreak Reporting System | USA | ~27.000 | 1998 -- 2023 |
| **FDA CAERS** | Center for Adverse Event Reporting | USA | ~108.000 | 2004 -- 2024 |

**Gesamt: ~221.000 Datensaetze** von sechs Behoerden aus drei Regulierungsjurisdiktionen.

RASFF-Daten sind auf reine Lebensmittelmeldungen gefiltert (ohne Futtermittel und Lebensmittelkontaktmaterialien). CDC NORS ist auf lebensmittelbedingte Ausbrueche begrenzt. FSIS-Eintraege sind sprachlich dedupliziert (nur Englisch).

## Star Schema

Das Data Warehouse folgt einem Kimball-Stil Star Schema mit fuenf Dimensionstabellen und fuenf Faktentabellen.

### Dimensionen

| Tabelle | Beschreibung | Wesentliche Design-Entscheidungen |
|---------|-------------|----------------------|
| `dim_date` | Kalenderdimension (2012--2026) | Beinhaltet FDA Fiscal Year (Okt--Sep Zyklus), Fiskalquartale |
| `dim_geography` | 150+ Standorte in USA, EU, UK | `IsEUMember` / `IsEFTA` Flags ermoeglichen Prä/Post-Brexit-Analysen |
| `dim_classification` | Schweregrade ueber alle Quellen | Bildet FDA Class I--III, RASFF Risk Decisions und UK FSA Alert Types auf eine einheitliche 1--10 Schweregradskala ab |
| `dim_product` | Produktbeschreibungen und Kategorien | 60+ Kategorien auf 12 uebergeordnete Produkttypen abgebildet |
| `dim_company` | Rueckrufende Unternehmen (FDA/FSIS) | ~5.000 verschiedene Unternehmen |

### Fakten

| Tabelle | Zeilen | Beschreibung |
|---------|-----:|-------------|
| `fact_recalls` | ~87.000 | Kern-Faktentabelle -- alle Rueckrufe von FDA, FSIS, RASFF und UK FSA mit doppelten Geografie-Schluesseln (Rueckrufort vs. Produktherkunft) |
| `fact_adverse_events` | ~108.000 | FDA CAERS Verbraucherbeschwerde-Berichte, nur Lebensmittel (ohne Kosmetik) |
| `fact_health_impact` | ~27.000 | CDC NORS Ausbruchsdaten mit Erkrankungs-, Hospitalisierungs- und Todeszahlen |
| `fact_yearly_summary` | ~120 | Aggregierte Rueckrufzahlen nach Jahr und Quelle fuer Trendanalysen |
| `fact_fsis_species` | ~50 | USDA Fleisch-/Gefluegelrueckrufe aufgeschluesselt nach Tierart |

### Dreistufige Rueckruf-Klassifikation

Jeder Rueckruf in `fact_recalls` wird durch eine eigene dreistufige Taxonomie klassifiziert:

```
RecallCategory          RecallGroup                 RecallSubgroup
─────────────────────────────────────────────────────────────────────
Product Contaminant     Biological Contamination    Listeria monocytogenes
                                                    Salmonella
                                                    E. coli O157:H7
                                                    Hepatitis A
                                                    Clostridium botulinum
                                                    ...
                        Allergens                   Milk
                                                    Peanuts
                                                    Tree Nuts
                                                    Soy
                                                    Wheat
                                                    ...
                        Chemical Contamination      Pesticides
                                                    Heavy Metals
                                                    Mycotoxins
                                                    Veterinary Drug Residues
                                                    ...
                        Foreign Objects             Metal Fragments
                                                    Glass
                                                    Plastic
                                                    ...

Process Issue           cGMP Issues                 Insanitary Conditions
                                                    Temperature Control
                                                    ...
                        Labeling Issues             Undeclared Allergens
                                                    Mislabeling
                                                    ...
```

Die Klassifikations-Engine deckt 50+ Pathogene, 90+ Allergen-Schluesselwoerter (FDA Big 9 + EU-spezifische Allergene), 70+ chemische Substanzen und gaengige Fremdkoerpertypen ab. Sie verarbeitet quellenspezifische Formate -- einschliesslich RASFF-Gefahrennotation wie `Listeria monocytogenes - {pathogenic micro-organisms}` -- und unterscheidet korrekt zwischen Kontaminationsereignissen und Prozess-/Kennzeichnungsmaengeln.

Diese Taxonomie stuetzt sich auf veroeffentlichte Klassifikationsmethodiken fuer Lebensmittelsicherheit (DeBeer et al. 2024, Blickem et al. 2025) und das IFSAC Food Categorization Scheme.

## Projektstruktur

```
food-recalls-database/
├── src/
│   ├── pipeline/
│   │   ├── transform_to_star_schema.py   # Kern-ETL: 1.910 Zeilen
│   │   ├── create_adverse_events.py      # FDA CAERS Verarbeitung
│   │   ├── create_fsis_species.py        # USDA Tierarten-Aufschluesselung
│   │   ├── create_yearly_summary.py      # Quellenuebergreifende Aggregation
│   │   ├── fetch_cdc_nors_data.py        # CDC NORS API Client
│   │   ├── fetch_fsis_data.py            # USDA FSIS API Client
│   │   └── upload_parquets_to_azure.py   # Azure Data Lake Upload
│   ├── validation/
│   │   ├── validate_star_schema.py       # Referenzielle Integritaetspruefungen
│   │   └── export_classification_review.py
│   ├── notifications/
│   │   ├── email_service.py              # SMTP-Benachrichtigungen (Jinja2 Templates)
│   │   ├── state_manager.py              # Duplikat-Benachrichtigungs-Vermeidung
│   │   └── templates/
│   └── utils/
│       └── logger.py                     # Rotierender Datei-Logger
├── config/
│   ├── Create_External_Tables.sql        # Synapse DDL (Star Schema)
│   ├── Refresh_External_Tables.sql       # Schema-Aktualisierung
│   ├── Validate_Data_Quality.sql         # SQL-Qualitaetspruefungen
│   ├── DAX_Measures.txt                  # Power BI Measures
│   └── email_settings.json
├── data/
│   ├── input/                            # Quelldaten (nicht im Repo)
│   └── output/
│       └── parquet/                      # Star Schema Parquet-Dateien
├── docs/
│   ├── AZURE_SOLUTION_DOKU.md
│   └── articles-academic/                # Referenzpublikationen
└── requirements.txt
```

**3.400+ Zeilen Python** in 14 Modulen. **1.600+ Zeilen SQL** in 5 Skripten. **236 Zeilen DAX** Measures fuer Power BI.

## Datenharmonisierung

Die zentrale Herausforderung dieses Projekts ist die Zusammenfuehrung von sechs strukturell unterschiedlichen Datenquellen in ein konsistentes Analysemodell. Jede Quelle hat ihr eigenes Schema, Datumsformat, Schweregradssystem und geografische Kodierung.

**Schemaabgleich** -- RASFF allein hat sein Exportformat 2021 geaendert (andere Spaltennamen, neue Felder, umstrukturierte Meldetypen). Die Pipeline verarbeitet sowohl das Pre-2021- als auch das Post-2021-Schema transparent und fuellt fehlende Spalten mit Null-Werten, um eine konsistente Ausgabe zu gewaehrleisten.

**Schweregradvereinheitlichung** -- Die FDA verwendet ein dreistufiges System (Class I = lebensbedrohlich, Class II = temporaere Gesundheitsfolgen, Class III = unwahrscheinliche Gesundheitsschaeden). RASFF verwendet eine Kombination aus Risikoentscheidung und Meldetyp. UK FSA hat eigene Warnungskategorien. Alle werden auf `dim_classification` mit einem einheitlichen Schweregrad und einem numerischen Score von 1 bis 10 abgebildet.

**Geografische Normalisierung** -- Laendernamen erscheinen in unterschiedlicher Gross-/Kleinschreibung und Schreibweise ueber alle Quellen hinweg ("THE NETHERLANDS", "Netherlands", "Italy", "ITALY"). Die Pipeline normalisiert diese und bildet sie auf eine gemeinsame Geografie-Dimension ab, wobei zwischen EU-Mitgliedern, EFTA-Staaten und Drittlaendern unterschieden wird.

**Datumsanalyse** -- Verarbeitet YYYYMMDD (FDA), YYYY-MM-DD (FSIS) und DD-MM-YYYY HH:MM:SS (RASFF) Formate. Die Datumsdimension umfasst sowohl Kalenderjahr als auch FDA Fiscal Year (Oktober--September) fuer regulatorisches Reporting.

## Azure-Infrastruktur

| Ressource | Name | Konfiguration |
|----------|------|---------------|
| Resource Group | `rg-food-recalls` | Germany West Central |
| Storage Account | `foodrecallsdata` | ADLS Gen2 (HNS), Standard_LRS, Hot Tier |
| Data Factory | `foodrecalls-adf` | 2 Pipelines, 7 Datasets, woechentlicher Trigger |
| Synapse Analytics | `foodrecalls-synapse` | Serverless SQL Pool, External Tables auf Parquet |

Data Factory uebernimmt die automatisierte Aufnahme von FDA- und FSIS-APIs mit paginierten Anfragen (1.000 Datensaetze pro Aufruf, Until-Schleife mit Offset-Tracking). Die uebrigen Quellen (RASFF, UK FSA, CDC NORS, FDA CAERS) werden ueber Python-API-Clients und Datei-Downloads aufgenommen.

Synapse Serverless SQL exponiert die Parquet-Dateien als External Tables, sodass Power BI DirectQuery ausfuehren kann, ohne Daten in einen dedizierten SQL Pool zu kopieren -- die Kosten bleiben unter 5$/Monat.

## Validierung & Qualitaet

`validate_star_schema.py` fuehrt nach jeder ETL-Ausfuehrung automatisierte Pruefungen durch:

- **Datensatzzaehlungspruefung** pro Quelle gegen erwartete Bereiche
- **Referenzielle Integritaet** -- erkennt verwaiste Fremdschluessel ueber alle Fakten-zu-Dimensions-Beziehungen
- **Datumsbereichsvalidierung** -- stellt sicher, dass keine Datensaetze ausserhalb des erwarteten 2012--2026 Fensters liegen
- **Null-Schwellenwert-Pruefungen** -- markiert Spalten, die 5% Null-Werte ueberschreiten
- **Klassifikationsverteilung** -- gibt die Aufteilung zwischen Product Contaminant und Process Issue Kategorien aus

SQL-seitige Validierung (`Validate_Data_Quality.sql`) verifiziert unabhaengig die gleichen Regeln gegen die Synapse External Tables, um Parquet-zu-SQL-Typmapping-Probleme zu erkennen.

### Parquet-Typmapping

Ein nicht offensichtliches Produktionsproblem, das dieses Projekt geloest hat: Pandas nullable Integers (`Int64` mit grossem I) werden als `float64` in Parquet gespeichert, nicht als `INT64`. Das bedeutet, dass Synapse External Table-Definitionen `FLOAT` statt `INT` oder `BIGINT` fuer jede Spalte verwenden muessen, die Null-Werte in den Quelldaten enthaelt -- andernfalls wirft Power BI `OLE DB` Typkonflikte. Die SQL-Skripte spiegeln diese korrigierten Mappings wider.

## Benachrichtigungen

Das Benachrichtigungssystem sendet E-Mail-Warnungen fuer neue Class I-Rueckrufe (lebensbedrohlich). Es verwendet Jinja2-Templates (HTML + Klartext-Fallback), SMTP mit TLS und exponentielle Backoff-Wiederholungslogik. Ein JSON-basierter State Manager verfolgt, welche Rueckrufe bereits gemeldet wurden, um doppelte Benachrichtigungen ueber Pipeline-Laeufe hinweg zu verhindern.

## Einrichtung

```bash
# Klonen und installieren
git clone https://github.com/leelesemann-sys/food-recalls-database.git
cd food-recalls-database
pip install -r requirements.txt

# Azure-Zugangsdaten konfigurieren
cp .env.example .env
# .env mit Ihrem Storage Account-Namen und Schluessel bearbeiten

# ETL-Pipeline ausfuehren
python src/pipeline/transform_to_star_schema.py

# Ergaenzende Faktentabellen generieren
python src/pipeline/create_adverse_events.py
python src/pipeline/create_fsis_species.py
python src/pipeline/create_yearly_summary.py

# Auf Azure Data Lake hochladen
python src/pipeline/upload_parquets_to_azure.py

# Ausgabe validieren
python src/validation/validate_star_schema.py
```

## Voraussetzungen

- Python 3.10+
- Azure-Abonnement (kostenlose Stufe ausreichend)
- Quelldatendateien in `data/input/` (nicht im Repo enthalten wegen Groesse)

Siehe `requirements.txt` fuer Python-Abhaengigkeiten.

## Referenzen

- DeBeer, J. et al. (2024). Analyzing FDA Food Recall Patterns Using Machine Learning.
- Blickem, C. et al. (2025). Food Safety Categorization and Risk Assessment Frameworks.
- IFSAC (Interagency Food Safety Analytics Collaboration). Food Categorization Scheme.
- FDA 21 CFR Part 7 -- Enforcement Policy (Recall Classification).
- EU RASFF -- Annual Reports and Notification Guidelines.

## Lizenz

Dieses Projekt wird fuer akademische und Forschungszwecke bereitgestellt.
