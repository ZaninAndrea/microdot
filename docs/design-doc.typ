#import "template.typ": report

#set figure(numbering: none)

#show: doc => report(
  title: "Design della piattaforma",
  longForm: true,
  doc,
)

= Architettura

L'architettura prevista per il sistema completo è la seguente:

#image("./diagrams/architecture.drawio.png", width: 100%)

= Requisiti
I requisiti di design del sistema sono i seguenti:
- Le uniche operazioni di scrittura supportate sono append e truncate di un log stream
- Il sistema deve avere requisiti minimi di RAM e CPU molto bassi, l'operatore può configurare le risorse allocate in modo da ottenere il miglior compromesso tra costo e performance
- Ha un query language per filtrare, con funzionalità simil regex e full text search
- Ha una web UI e/o remote TUI con gestione degli utenti, flusso di dati real time e navigazione dei dati storici
- Può riceve i dati in forma standard (#link("https://brandur.org/logfmt")[logfmt] o JSON)

Altri aspetti desiderabili ma non obbligatori in una prima fase sono:
- Compatibilità con il protocollo di ingestion di ElasticSearch, Loki o InfluxDB
- Può salvare i dati su blob storage
- Può essere deployed in multiple istanze che sono eventually consistent
- Possibilità di salvare metriche oltre che logs (il formato di input potrebbe essere lo stesso, semplicemente omettendo il campo messaggio)

= Storage engine
Lo storage engine deve essere un LSM-tree o simili:
- Non va bene salvare i dati semplicemente nel WAL, perché potrebbero arrivare out-of-order, quindi devo usare un LSM-tree per poterli riordinare dato che l’accesso sarà in order
- LSM-tree per poter avere high ingest rate e avere batched sequential writes
- Column storage per miglior compressione dei dati e supporto a sparse columns
- Idealmente il partitioning dovrebbe essere ottimizzato automaticamente dal sistema. Dovrebbe dividere le partizioni per mantenere la dimensione in un certo range
  - Mentre dividi l’lsm-tree i dati nuovi vengono aggiunti ad altre stables già nelle partizioni nuove
  - Le sstables verranno poi compattate normalmente
- L'eliminazione dei valori è supportata con un sistema di tombstone. Le tombstone sono salvate in una zona specifica del file su disco, in questo modo non sprechiamo spazio: per ciascuna tombstone salviamo solo il valore della chiave e nella zona con dati veri non serve aggiungere delle flag "tombstone".

Molti time-series DB usano delle variazioni di LSM-tree che risolvono alcune problematiche specifiche, come ad esempio:
- Possibilità di droppare efficientemente i dati più vecchi
  - Alcuni DB dividono i dati in shard in modo da poter eliminare una shard intera semplicemente droppando una cartella/file
  - Altri DB implementano un TTL che viene applicato durante la compaction
- Evitare di avere troppe file handle aperte, in particolare per le implementazioni che fanno sharding

In InfluxDB ciascuna SSTable contiene un blocco per ogni series e ciascun blocco contiene compressi i dati di una timeseries. La scelta della compressione e l'elenco delle colonne è a livello di blocco.

Gerarchia fisica di storage:

- DBMS: Un’istanza o un cluster di istanze del DMBS
- Database: Ciascun DBMS contiene vari Database, ogni database ha una gestione separata di autorizzazioni
- Partition: I dati di ciascun database sono partizionati in base al timestamp (e.g. su base giornaliera)
- DataDB e IndexDB: ciascuna partition contiene un LSMTree per i dati e un LSMTree per gli indici

Organizzazione logica dei dati:

- Log: l’unità base è un log, che può essere structured o unstructured
- Stream: ciascun log appartiene ad uno stream, questo corrisponde tipicamente al processo che lo ha generato (e.g. il container docker).
  - Ciascuno stream può avere delle regole diverse per la retention window idealmente
- Database: gli stream sono raggruppati in database, che corrispondono tipicamente al servizio/prodotto a cui appartengono

== Compaction

L'obiettivo della compaction è convertire un formato rapido da scrivere in un formato rapido da leggere.

Per le performance delle implementazioni base su LSM-trees è molto importante il design della procedura di compaction.

InfluxDB ad esempio usa diverse strategie di compaction per SSTable recenti e per SSTable vecchie in modo da bilanciare l'uso di risorse e l'aumento di read-performance.

== Altre idee
- VictoriaLogs non usa un WAL, questo riduce l’uso di disco ma introduce la possibilità di perdere i dati ricevuti da poco quando crasha.
- Riconoscere in automatico dati con tipi specifici (e.g. int, float, datetime, …), parsarli e salvarli parsati. Questo dovrebbe aiutare a ridurre lo storage utilizzato e forse anche a semplificare l’indicizzazione.
- Riconoscere in automatico dei pattern nei log (e.g. pezzi di frasi ricorrenti) e creare dei template per salvarli una volta e poi sostituirli con un placeholder.

= Indici

Dovremmo tenere uno sparse index per timestamp in modo da poter iniziare a leggere il blocco dal punto giusto, senza dover leggere tutto il blocco.

== Full text search

Le opzioni possono essere:
- inverted index - trigram index: https://swtch.com/~rsc/regexp/regexp4.html - dovrebbe essere particolarmente adatta per il caso dei log, ovvero tanti documenti molto piccoli
- trie or HAT trie
- bloom filter

=== Bloom filter
bloom filter per saltare tutti i blocchi che non contengono una parola/trigram


Utilizzando un dataset pubblico di logs ho stimato che il numero di trigram unici in tutti i log potrebbe essere ~5000.

Potremmo salvare i timestamp (8byte) individualmente finché sono meno di 1M per un dato trigram, poi invece passiamo ad usare un bloom filter.

Nel bloom filter salviamo i timestamp di tutti i log che contengono quel trigram, il timestamp lo salviamo con precisione al minuto, poi all’ora, poi al giorno, poi al mese. Così possiamo fare una “ricerca” nel filtro prima scansionando per mese, poi per giorno dei mesi presenti, …

Ci aspettiamo di salvare dati fino a 5 anni di storico, quindi il bloom filter può contenere fino a 2.6M di timestamp diversi.

Per avere un false positive rate di 1.0E-6 il bloom filter deve occupare 9 MB.

Simulando questo setup su dataset di logs di varie dimensioni ignorando la possibilità di compressione degli indici e ignorando il fatto che un trigram può apparire più volte nello stesso minuto (se appare due volte lo salvo due volte), ho ottenuto queste dimensioni:

#table(
  columns: 4,
  [Dataset size], [Inverted index size], [Bloom filter size], [Note],
  [17GB], [14GB], [17GB], [loghub-HDFS2],
  [1.6GB], [4GB], [2.4GB], [loghub-HDFS],
  [2.9GB], [5GB], [4GB], [loghub-spark, contiene tanti unique trigrams],
)

Per evitare di dover sovrascrivere tanti dati continuamente potremmo salvare una gerarchia di bloom filter:

- Un bloom filter globale per ciascun trigram che specifica quali giorni contengono quel trigram
  - Viene aggiornato solo una volta al giorno e dovrebbe pesare circa 7KB
- Un bloom filter per ciascun giorno che specifica quali minuti contengono quel trigram
  - Quelli più vecchi di un giorno non vengono più toccati
  - Dovrebbe pesare circa 5KB

Con questa soluzione il cutoff per quando passare da inverted index a bloom filter diventa ci circa 700 entries (ovvero 1000 minuti in un giorno o 1000 giorni in un global), perché il bloom filter è più piccolo. Non ha molto senso, perché a questo punto quando usiamo il bloom filter il 50% delle entries possibile è parte del bloom filter.

= Compressione

- Comprimiamo ciascun blocco separatamente
- Huffman coding per le colonne che hanno cardinalità bassa, come “carattere” dell’huffman coding uso i possibili valori della colonna
  - Alternativa più rapida ma meno compressa: dictionary compression (la usa timescaledb)
- Per i timestamp salviamo solo la parte che varia all’interno del blocco (e.g. l’anno lo salviamo come metadato nel blocco, non lo ripetiamo tutte le volte)
- Copiare parquet, zip o altro
  - https://docs.tigerdata.com/use-timescale/latest/hypercore/compression-methods/: _For integers, timestamps, and other integer-like types TimescaleDB uses a combination of delta encoding, delta-of-delta, simple 8-b, and run-length encoding._
  - #link(
      "https://docs.influxdata.com/influxdb/v1/concepts/storage_engine/#compression",
    )[InfluxDB compression]
    - Timestamp: Delta-of-delta encoding
    - Field Value: Because Field Values in a single data block are definitely of different data types, different compression algorithms can be used for different data types.
    - Float: the Float compression algorithm of Gorilla
    - Integer: Delta Encoding + Zigzag Conversion + RLE / Simple8b / None
    - String: Snappy Compression
    - Boolean: Bit packing
  - LZ4 encoding

= Memory management
Dovrebbe esserci una buffer pool globale che eroga buffer in memory fino ad un limite stabilito nella configurazione del database (o inferito da request/limit di k8s), poi passa ad erogare disk-backed buffers
- Potrebbe esserci anche una gestione della priority se necessario, in modo da dare gli in-memory buffers preferenzialmente ai bottleneck di performance

= Altro

- Per il benchmarking posso usare #link("https://vector.dev/docs/reference/configuration/sources/demo_logs/#interval")[Vector.dev] come generatore di log per i test
- Quando microdot si avvia la prima volta potrebbe eseguire in automatico un tuning dei vari parametri per ottimizzare le performance.
