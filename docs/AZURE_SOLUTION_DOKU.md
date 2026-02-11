# FDA Food Recall - Azure Solution Dokumentation

## Übersicht

Diese Lösung erfasst FDA Food Recall-Daten über die offizielle FDA API und speichert sie in Azure Data Lake Storage Gen2. Die Daten werden anschließend mit Python transformiert und für Power BI aufbereitet.

---

## Architektur

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│   FDA REST API  │────▶│  Azure Data Factory  │────▶│  ADLS Gen2 (raw/)   │
│ (enforcement)   │     │  PL_Ingest_FDA       │     │  JSON-Dateien       │
└─────────────────┘     └──────────────────────┘     └──────────┬──────────┘
                                                                │
                                                                ▼
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│   Power BI      │◀────│  csvdat.py           │◀────│  ADLS Gen2 (gold/)  │
│   Dashboard     │     │  (Transformation)    │     │  Parquet + CSV      │
└─────────────────┘     └──────────────────────┘     └─────────────────────┘
```

---

## Azure-Ressourcen

### Resource Group
| Eigenschaft | Wert |
|-------------|------|
| Name | `rg-food-recalls` |
| Subscription | Azure-Abonnement 1 |
| Region | Germany West Central |

### Storage Account (ADLS Gen2)
| Eigenschaft | Wert |
|-------------|------|
| Name | `foodrecallsdata` |
| Typ | StorageV2 mit HNS (Hierarchical Namespace) |
| Access Tier | Hot |
| SKU | Standard_LRS |
| Region | Germany West Central |
| URL | `https://foodrecallsdata.dfs.core.windows.net/` |

### Container: `raw`
| Ordner/Datei | Beschreibung |
|--------------|--------------|
| `part_0.json` - `part_10000.json` | Rohdaten von FDA API (je 1000 Records) |
| `gold/fda_recalls_final.parquet` | Transformierte Daten (2.4 MB) |
| `gold/fda_recalls_final.csv` | CSV-Export für Excel (7.6 MB) |
| `csv/fda_output.csv` | Legacy CSV |
| `parquet/fda_results.parquet` | Legacy Parquet |

### Data Factory
| Eigenschaft | Wert |
|-------------|------|
| Name | `foodrecalls-adf` |
| Region | Germany West Central |
| Version | V2 |

---

## Data Factory Pipeline: `PL_Ingest_FDA`

### Funktion
Inkrementelle Datenabfrage von der FDA Enforcement API mit Paginierung (1000 Records pro Request).

### Pipeline-Logik
```
1. Until-Loop (bis Offset > 11000)
   ├── CopyData Activity
   │   ├── Source: FDA REST API
   │   └── Sink: JSON-Datei in ADLS
   ├── PrepareNextOffset (+1000)
   └── ApplyOffset
```

### Parameter
| Parameter | Typ | Beschreibung |
|-----------|-----|--------------|
| `FDA_API_Key` | String | API-Schlüssel für FDA OpenFDA |

### Variablen
| Variable | Typ | Beschreibung |
|----------|-----|--------------|
| `Offset` | Integer | Aktuelle Position für Paginierung |
| `NewOffset` | Integer | Berechneter nächster Offset |
| `RowsRead` | Integer | Anzahl Records pro Request (1000) |

---

## Linked Services

### LS_FDA_API
| Eigenschaft | Wert |
|-------------|------|
| Typ | REST Service |
| URL | `https://api.fda.gov/food/` |
| Auth | Anonymous |

### LS_ADLS_Gen2
| Eigenschaft | Wert |
|-------------|------|
| Typ | Azure Blob FS (ADLS Gen2) |
| URL | `https://foodrecallsdata.dfs.core.windows.net/` |
| Auth | Account Key (verschlüsselt) |

---

## Datasets

### DS_FDA_Source (REST)
- **Linked Service:** LS_FDA_API
- **Relative URL:** `enforcement.json?api_key={key}&limit=1000&skip={offset}&search=report_date:[20190101+TO+20260115]`
- **Parameter:** `p_api_key`, `p_offset`

### DS_FDA_JSON_Sink (ADLS)
- **Linked Service:** LS_ADLS_Gen2
- **Format:** JSON
- **Pfad:** `raw/part_{offset}.json`

---

## Python-Transformation (csvdat.py)

### Funktion
1. Liest alle JSON-Dateien aus `raw/`
2. Entfernt Dubletten (`recall_number`)
3. **NEU:** Sendet Email-Benachrichtigungen für Class I Recalls
4. Bereinigt Struktur (entfernt `openfda`)
5. Konvertiert Datumsfelder
6. Exportiert nach `gold/`:
   - `fda_recalls_final.parquet` (für Power BI)
   - `fda_recalls_final.csv` (für Excel)

### Ausführung
```bash
python csvdat.py
```

---

## Datenfluss

```
FDA API (enforcement.json)
    │
    │ limit=1000, skip=0,1000,2000...
    ▼
Data Factory Pipeline
    │
    │ 12 JSON-Dateien (part_0 bis part_10000)
    ▼
ADLS Gen2 Container "raw"
    │
    │ csvdat.py
    ▼
ADLS Gen2 Container "raw/gold/"
    │
    │ Parquet + CSV
    ▼
Power BI / Excel
```

---

## Datenmenge

| Metrik | Wert |
|--------|------|
| Rohdaten (JSON) | ~12 MB (12 Dateien) |
| Parquet (Gold) | 2.4 MB |
| CSV (Gold) | 7.6 MB |
| Records | ~11.000+ |
| Zeitraum | 2019-01-01 bis 2026-01-15 |

---

## Email-Benachrichtigung (NEU)

Bei jedem Durchlauf von `csvdat.py` werden neue Class I Recalls erkannt und per Email gemeldet:

- **Konfiguration:** `config/email_settings.json`
- **State:** `state/notified_recalls.json`
- **Logs:** `logs/pipeline.log`

---

## Sicherheit

| Komponente | Schutz |
|------------|--------|
| Storage Account Key | In `.env` (nicht im Repo) |
| FDA API Key | Als Pipeline-Parameter |
| SMTP Credentials | In `.env` (nicht im Repo) |

---

## Synapse Serverless SQL External Tables

### Datenbank
| Eigenschaft | Wert |
|-------------|------|
| Name | `FoodRecallsDB` |
| Data Source | `GoldDataLake` |
| File Format | `ParquetFormat` (Snappy-Kompression) |
| Location | `abfss://raw@foodrecallsdata.dfs.core.windows.net/gold/` |

### Star Schema Tabellen

**Dimensions:**
- `dim_date` - Datumsdimension (DateKey, Year, Quarter, Month, etc.)
- `dim_geography` - Länder und Regionen (GeographyKey, Country, State, Region)
- `dim_classification` - Recall-Klassifikationen (ClassificationKey, SeverityLevel, SeverityScore)
- `dim_product` - Produkte (ProductKey, ProductName, ProductCategory, ProductType)
- `dim_company` - Unternehmen (CompanyKey, CompanyName, City, State)

**Facts:**
- `fact_recalls` - Hauptfakten-Tabelle mit allen Recalls (~87.000 Zeilen)
- `fact_health_impact` - CDC NORS Outbreak-Daten
- `fact_yearly_summary` - Jährliche Aggregation pro Quelle
- `fact_fsis_species` - USDA FSIS Recalls nach Tierart
- `fact_adverse_events` - FDA CAERS Adverse Event Reports (~108.000 Zeilen)

### Views für Power BI
- `vw_recalls_analysis` - Denormalisierte Star Schema View für Recall-Analyse
- `vw_health_impact_analysis` - CDC NORS View

---

## ⚠️ WICHTIG: Parquet-zu-SQL Datentyp-Mappings

Diese Mappings sind **kritisch** für Synapse External Tables. Fehlerhafte Typen führen zu Power BI Fehlern.

### Bekannte Probleme und Lösungen

| Tabelle | Spalte | Parquet-Typ | SQL-Typ | Grund |
|---------|--------|-------------|---------|-------|
| `fact_yearly_summary` | `PoundsRecalled` | INT64 | **BIGINT** | Parquet INT64 ≠ SQL FLOAT |
| `dim_company` | `EstablishmentNumber` | INT32 | **INT** | Parquet INT32 ≠ SQL NVARCHAR |
| `fact_adverse_events` | `DateKey` | DOUBLE | **FLOAT** | Pandas nullable int → float64 |
| `fact_adverse_events` | `ConsumerAge` | DOUBLE | **FLOAT** | Pandas nullable int → float64 |
| `fact_fsis_species` | `PoundsRecalled` | DOUBLE | **FLOAT** | Pandas nullable int → float64 |
| `fact_recalls` | `OriginGeographyKey` | DOUBLE | **FLOAT** | Pandas nullable int → float64 |
| `dim_date` | `Date` | STRING | **VARCHAR(10)** | Parquet BYTE_ARRAY = String |
| `fact_recalls` | `RecallDate` | STRING | **VARCHAR(50)** | Parquet BYTE_ARRAY = String |

### Pandas → Parquet → SQL Konvertierungsregeln

```
Pandas dtype          Parquet Physical Type    Synapse SQL Type
─────────────────────────────────────────────────────────────────
int64                 INT64                    BIGINT
int32                 INT32                    INT
Int64 (nullable)      DOUBLE (float64!)        FLOAT ⚠️
float64               DOUBLE                   FLOAT
bool                  BOOLEAN                  BIT
string/object         BYTE_ARRAY               NVARCHAR(n)
datetime64            INT96 or STRING          VARCHAR(50) ⚠️
```

### ⚠️ Häufigster Fehler: Nullable Integers in Pandas

Pandas speichert **nullable integers** (`Int64` mit großem I) als `float64` in Parquet!

```python
# Problem: Spalte mit NaN-Werten
df['DateKey'] = df['DateKey'].astype('Int64')  # Nullable integer
# → Parquet speichert als DOUBLE (float64)
# → SQL muss FLOAT sein, nicht INT!
```

**Lösung:** Bei Spalten mit möglichen NULL-Werten in SQL FLOAT verwenden.

### Power BI Fehlermeldung bei falschem Typ

```
OLE DB- oder ODBC-Fehler: Column 'ColumnName' of type 'WRONG_TYPE'
is not compatible with external data type 'Parquet physical type: ACTUAL_TYPE',
please try with 'CORRECT_TYPE'.
```

---

## Datenquellen

| Quelle | Typ | Zeitraum | Records |
|--------|-----|----------|---------|
| FDA | US Food Recalls | 2012-2026 | ~28.000 |
| FSIS | USDA Meat/Poultry Recalls | 2012-2024 | ~1.000 |
| RASFF | EU Food Alerts | 2002-2026 | ~56.000 |
| UK FSA | UK Food Alerts | 2015-2026 | ~1.000 |
| CDC NORS | US Outbreak Data | 1998-2023 | ~27.000 |
| FDA CAERS | Adverse Event Reports | 2004-2024 | ~108.000 |

### RASFF Filter (2026-01-23)
- **Nur "food"** - "feed" und "food contact materials" wurden ausgeschlossen
- Reduzierung: ~64.000 → ~56.000 Records

---

## Nächste Schritte (optional)

1. **Trigger einrichten:** Tägliche/wöchentliche Ausführung der Pipeline
2. **Monitoring:** Azure Monitor Alerts bei Pipeline-Fehlern
3. **Power BI:** DirectQuery auf Parquet-Datei
