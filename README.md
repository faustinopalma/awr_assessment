# AWR Assessment

Script Python per estrarre metriche dai report Oracle **AWR (Automatic Workload Repository)** in formato HTML e raccoglierle in un unico file Excel. Il risultato facilita il confronto tra database, istanze e intervalli di osservazione durante un assessment di prestazioni e dimensionamento.

Lo script analizza report gia' disponibili: non si collega a Oracle, non genera report AWR e non modifica il database.

## Script disponibili

- **[AWR_reader.py](AWR_reader.py)**: versione piu' completa, consigliata per l'uso. Include release Oracle, RAC, piattaforma, intervallo degli snapshot, informazioni Exadata e statistiche CPU dell'host.
- **[main.py](main.py)**: implementazione alternativa con meno campi e messaggi di esempio/debug. Non e' un launcher di `AWR_reader.py`.

Entrambi cercano gli input in `./AWR` e scrivono in `./output/awr_data.xlsx`. I percorsi sono relativi alla **directory da cui si esegue il comando**, non alla posizione del file Python.

## Prerequisiti

- Python 3 con `pip` e supporto agli ambienti virtuali.
- Report Oracle AWR HTML con intestazioni e sezioni in inglese.
- Pacchetti `beautifulsoup4`, `pandas`, `lxml` e `openpyxl`.

Il codice usa conversioni di Series pandas in `float` e accessi posizionali legacy: per questo l'esempio limita pandas a una versione precedente alla 3. Non e' disponibile una matrice di compatibilita' verificata per le versioni di Python, pandas o Oracle.

## Installazione

Da PowerShell su Windows:

```powershell
git clone https://github.com/faustinopalma/awr_assessment.git
Set-Location awr_assessment
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install beautifulsoup4 "pandas<3" lxml openpyxl
```

I comandi usano direttamente l'interprete dell'ambiente virtuale, senza richiedere l'attivazione. Su Linux/macOS il percorso equivalente e' `.venv/bin/python`.

## Preparazione degli input

Creare le cartelle `AWR` e `output` nella radice del progetto:

```powershell
New-Item -ItemType Directory -Force -Path AWR, output | Out-Null
```

Inserire i report HTML in `AWR`, anche in sottocartelle. Per esempio:

```text
awr_assessment/
|-- AWR_reader.py
|-- main.py
|-- AWR/
|   |-- awr_database_01.html
|   `-- database_02/
|       `-- awr_database_02.html
`-- output/
```

La scansione e' ricorsiva e **non filtra le estensioni**: tutti i file presenti in `AWR` vengono trattati come report. Non inserire PDF, file di testo, archivi o altri file non pertinenti. Deve essere presente almeno un report valido. La cartella `output` deve esistere: lo script non la crea.

## Esecuzione

Dalla radice del progetto:

```powershell
.\.venv\Scripts\python.exe .\AWR_reader.py
```

Non sono previsti argomenti da riga di comando o file di configurazione. Durante l'elaborazione vengono stampati i percorsi dei report e alcuni avvisi sulle sezioni non trovate.

Al termine viene creato **`output/awr_data.xlsx`**, con una riga per ciascun report e una colonna iniziale contenente l'indice pandas. Le esecuzioni successive sovrascrivono il file: chiuderlo in Excel prima di avviare lo script e conservare altrove gli output da mantenere.

## Dati estratti

| Categoria | Campi principali di `AWR_reader.py` |
| --- | --- |
| Origine e database | `AWR file`, `DB Name`, `Release`, `RAC` |
| Istanza e host | `Instance Name`, `Startup Time`, `Platform`, `Host Name` |
| Intervallo | `Begin Snap:`, `End Snap:`, `Elapsed Time (mins)`, `DB Time (mins)` |
| Exadata | `Exadata`, `Exadata Model` quando la sezione e' presente |
| CPU | `DB CPU (s)`, `CPUs`, `Cores`, `BUSY_TIME`, `IDLE_TIME`, `Host CPU %Idle`, `%Busy CPU` |
| Memoria | `Memory (GB)`, `SGA use (MB)`, `PGA use (MB)` |
| I/O | `Read Throughput (MB/s)`, `Write Throughput (MB/s)`, `Read IOPS`, `Write IOPS` |

La CPU del database viene letta da `Wait Classes by Total Wait Time`, oppure dalla somma di `DB CPU (s)` in `Service Statistics`. Per SGA/PGA vengono usati i valori iniziali del riepilogo quando disponibili, altrimenti le sezioni `SGA Memory Summary` e `PGA Aggr Target Stats`.

La presenza della sezione `Exadata Storage Server Model` determina il valore `YES` di `Exadata`. Il valore `NO` indica che quella sezione non e' stata trovata: non e' una verifica indipendente dell'infrastruttura.

## Metriche calcolate

| Colonna | Calcolo |
| --- | --- |
| `Total Throughput (MB/s)` | Throughput di lettura + throughput di scrittura |
| `Total IOPS` | IOPS di lettura + IOPS di scrittura |
| `%DB Time of Elapsed Time` | DB Time / tempo trascorso |
| `CPU total capacity (s)` | Tempo trascorso in minuti x 60 x numero di CPU logiche |
| `%DB CPU of server capacity` | DB CPU / capacita' totale CPU |
| `ORA use (GB)` | (SGA in MB + PGA in MB) / 1024 |
| `source CPU HT factor` | CPU logiche / core fisici |
| `%idle CPU` | IDLE_TIME / (BUSY_TIME + IDLE_TIME) x 100; solo in `AWR_reader.py` |

**Attenzione all'interpretazione:**

- Nonostante il prefisso `%`, `%DB Time of Elapsed Time` e `%DB CPU of server capacity` sono rapporti **non moltiplicati per 100**. Per esempio, `0.25` corrisponde al 25%.
- DB Time aggrega il tempo delle sessioni e puo' superare il tempo trascorso: il suo rapporto non e' una percentuale di utilizzo CPU.
- Il codice divide i byte letti per `1024^2` (MiB/s) e i byte scritti per `1000^2` (MB/s). Le etichette sono entrambe `MB/s`, ma il totale somma grandezze con convenzioni diverse.
- `Memory (GB)` e `ORA use (GB)` usano conversioni binarie, quindi i valori sono di fatto in GiB. SGA/PGA non rappresentano necessariamente il picco di memoria durante l'intervallo.
- Le metriche I/O provengono dalla colonna `per Second` di `Instance Activity Stats`, non rappresentano picchi istantanei.

## Limiti e problemi comuni

Il parser dipende da nomi di sezioni, colonne e statistiche specifici. Non tutti i formati o le versioni dei report AWR sono supportati; in particolare, non bisogna presumere compatibilita' con report globali RAC, report localizzati o layout diversi da quelli attesi.

| Problema | Verifica |
| --- | --- |
| `ModuleNotFoundError` | Installare i pacchetti usando lo stesso interprete con cui si esegue lo script. |
| Errore sulla cartella di destinazione | Creare `output` e verificare di eseguire il comando dalla radice del progetto. |
| `PermissionError` sull'Excel | Chiudere `awr_data.xlsx` in Excel e verificare i permessi di scrittura. |
| `KeyError`, `AttributeError` o conversioni numeriche fallite | Verificare le sezioni del report, i nomi delle colonne, il contenuto numerico e la versione di pandas. Una cartella `AWR` vuota puo' causare un `KeyError`. |
| `UnicodeDecodeError` | I file vengono aperti con la codifica predefinita dell'ambiente; verificare la codifica dei report. |

Alcune sezioni mancanti producono soltanto un avviso e campi vuoti; altre interrompono l'esecuzione. Non esiste gestione degli errori per singolo report: un file non valido puo' impedire la creazione dell'intero output. Anche intervalli nulli o dati hardware incompleti possono rendere non significativi i rapporti calcolati.

Il repository non include report di esempio o test automatici. Verificare l'output su report rappresentativi prima di usarlo per decisioni di dimensionamento. I report e l'Excel possono contenere nomi di database, host e informazioni infrastrutturali: non pubblicarli nel repository senza averne verificato la riservatezza.

## Licenza

Distribuito con licenza [MIT](LICENSE).
