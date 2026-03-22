#import "template.typ": report

#set figure(numbering: none)

#show: doc => report(
  title: "Design della piattaforma",
  longForm: true,
  doc,
)

= Oggetti

Gli oggetti salvati su S3 sono:
- WAL directory: contiene i log non ancora compattati, che vengono letti per servire le query più recenti.
  - Viene creato un nuovo file ogni 1MB o ogni 10 secondi, a seconda di quale condizione si verifica prima.
  - Per evitare di dover leggere troppi file, i WAL files vengono compattati in file più grandi a gruppi di 10 file, fino ad una dimensione di 100MB.
- Stream directory: quando i WAL files superano i 100MB vengono spezzati in base allo stream. Per ogni stream viene creato un archive file separato.
  - Man mano che vengono creati nuovi archive file per un dato stream, i file di uno stesso stream vengono gestiti come un LSM-tree



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

Ciascuno stream viene salvato in vari file, sharded in base al timestamp di ricezione. I file sono column-oriented.

== Compaction

L'obiettivo della compaction è convertire un formato rapido da scrivere in un formato rapido da leggere.

Per le performance delle implementazioni basate su LSM-trees è molto importante il design della procedura di compaction.

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
  - skip pointers
  - compressione delle posting lists
  - p for delta
- trie or HAT trie
- bloom filter

Usiamo un unico inverted index per tutti gli stream, teniamo un WAL unico per ricostruire l'inverted index in caso di failure.
Gli stream vengono inizialmente salvati in un formato non compresso che possiamo scrivere facendo append un record alla volta; poi con il primo merge di due file avviene la compressione per blocchi.
Durante il primo merge vengono inferite le colonne presenti nello stream e

Oltre ai valori di una colonna viene salvata una bitmap che indica quali valori sono null. Nel blocco che codifica i valori vengono codificati solo quelli non null. Durante la decodifica bisogna utilizzare la bitmap per sapere a che indice corrisponde ciascun valore.
Le bitmap sono compresse con roaring bitmaps.

Potrei usare le bitmap anche per rappressentare le posting list.

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
    - Boolean: Bit packing. Roaring bitmaps
  - LZ4 encoding

Il numero di righe in un blocco potrebbe essere più grande per i file che contengono log vecchi, così da migliorare la compressione al costo di una lettura più lenta (che però non è un grosso problema perché i log vecchi vengono letti meno spesso).

= Memory management
Dovrebbe esserci una buffer pool globale che eroga buffer in memory fino ad un limite stabilito nella configurazione del database (o inferito da request/limit di k8s), poi passa ad erogare disk-backed buffers
- Potrebbe esserci anche una gestione della priority se necessario, in modo da dare gli in-memory buffers preferenzialmente ai bottleneck di performance

= Altro

- Per il benchmarking posso usare #link("https://vector.dev/docs/reference/configuration/sources/demo_logs/#interval")[Vector.dev] come generatore di log per i test
- Quando microdot si avvia la prima volta potrebbe eseguire in automatico un tuning dei vari parametri per ottimizzare le performance.

= ID

Ogni log ha un ID univoco, che è formato da ID dello stream + timestamp + un numero progressivo per distinguere log con lo stesso timestamp. Ogni stream ha un proprio contatore per il numero progressivo. Dato che i log di ciascuno stream vengono inseriti in ordine di timestamp, questo ID è strictly increasing.

Nel caso di log che arrivano out of order, assegnamo come ID l'ID precedente+1 in questo modo gli ID rimangono unici e strictly increasing; il timestamp out of order lo salviamo in un apposito campo del log `_ooo_ts` in modo da poter mostrare nella UI che il log è arrivato in ritardo.

In fase di query possiamo allargare l'intervallo di timestamp per includere anche i log out of order, questo ci permette di avere un cutoff di ritardo diverso per ciascuna query.

= Stream open

Quando viene aperto uno stream invece di rileggere tutto il WAL possiamo leggere dal fondo fino a trovare il primo \n e poi leggere l'ultimo log partendo da lì. In questo modo riotteniamo il precedente ID. Invece di usare il numero di log possiamo usare la file size come limite per comprimere il WAL.
