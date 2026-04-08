Principi:
- Deve essere possibile fare un deployment con un singolo processo.
- Lo storage è fatto su Object Storage.
- Le DevOps devono essere estremamente facili:
  - La configurazione del sistema deve essere estramemente semplice e il più possibile automatica.
  - Si deve poter rimuovere tutti i nodi senza perdere dati.
  - Si devono poter aumentare o ridurre i nodi senza problemi di quorum.

Elementi principali:
- I log vengono ricevuti e buffered in RAM, periodicamente vengono scritti su S3 in un file di WAL. Il write viene acked solo dopo che il file di WAL è stato scritto su S3.
- Un sistema di queue gestisce i job di sharding, che spezzano i file di WAL in base allo stream, e quelli di compaction, che compattano i livelli degli LSM.
  - La richiesta di compaction è salvata in un file `queue/0.json` che tiene la lista dei job da eseguire. Ciascun nodo può aggiungere job alla coda, prendere un job e rimuovere un job; tutte le operazioni sono fatte con compare-and-swap su S3.
  - In un secondo step per aumentare il throughput della coda potremmo fare sharding della coda su più file. In questo caso ciascun job andrebbe aggiunto ad una coda a caso, ottenendo quindi un load balancing delle scritture, che evita la contention delle CAS. Il numero di file della coda dovrebbe essere gestito dinamicamente per bilanciare contention e numero di operazioni di lettura necessarie per trovare un job.
- Quando il client fa una query, il nodo risponde basandosi solo sui file in Blob Storage e non sui dati in RAM, dato che i dati in RAM non sono ancora stati confermati.

Object storage:
- `wal/`: contiene vari file di log non compattati. I file sono read-only. Ciascun file è scritto in modo sequenziale da un solo processo, processi diversi scrivono file diversi.
- `stream/`: contiene vari file di log compattati in formato LSM column-oriented.
- `trigram/`: contiene un trigram index di tutti i log (esclusi quelli ancora nel WAL). Il trigram index è salvato come LSM.
- `queue/`: dati relativi alla coda dei jobs.

= Design della queue
Una coda è salvata su S3 come una directory con al suo interno vari file.

Globalmente è definito un parametro `maxJobsPerFile`, che indica il numero massimo di job che possono essere inseriti in un file.

== File format
Ciascun file ha come nome `<ID>.json` e segue questo schema:

```json
{
  "status": "active" | "splitting" | "preparing_split",
  "lockedUntil": "<timestamp>" | unset, // quando scade il lock "splitting" del file, usato per timeout
  "splitInto": "<UUID>" | unset, // nome del nuovo file in cui stiamo splittando
  "splitFrom": "<UUID>" | unset, // nome del file da cui stiamo splittando
  "jobs": [
    {
      "id": "<UUID>",
      "status": "pending" | "in_progress",
      "lockedUntil": "<timestamp>" | unset, // quando scade il lock "in_progress" del job, usato per timeout
      "payload": { ... } // arbitrary data
    },
    ...
  ]
}
```

== Add job
Quando un nodo vuole aggiungere un job:
- lista i file nella directory usando l'API di S3
  - Se non ci sono file il nodo crea un nuovo file con un job al suo interno e lo scrive su S3. FINE PROCEDURA
- sceglie un indice a caso da cui partire a controllare i file
- legge uno alla volta i file, partendo dall'indice scelto, e controlla se lo status è "active". Continua finché non trova un file con status "active" o "preparing_split".
  - Se nessun file è "active" o "preparing_split", il nodo attende un exponential backoff e ritenta l'operazione di inserimento del job da zero.
  - Se ci sono più di un file, allora il nodo ignora i file con meno di `maxJobsPerFile / 4` job, in modo che se ci sono troppi file quasi vuoti, alcuni vengano rimossi. Se sono tutti sotto `maxJobsPerFile / 4`, allora il nodo inserisce il job nel file con l'ID più basso.
- modifica con CAS il file attivo aggiungendo il job alla fine della coda. Se il numero di jobs supera `maxJobsPerFile` il nodo imposta anche `status: "preparing_split"`. Se il file è stato modificato da un altro nodo, il nodo ritenta l'operazione di inserimento del job da zero.

Per efficienza i job vengono buffered in RAM e scritti su S3 in batch, il job viene acked solo dopo che il file è stato aggiornato su S3.

== Take job
Quando un nodo vuole prendere un job:
- lista i file nella directory usando l'API di S3
- Sceglie un indice a caso da cui partire a controllare i file
- legge uno alla volta i file, partendo dall'indice scelto, e controlla se lo status è "active". Continua finché non trova un file con status "active".
  - Se nessun file è "active", il nodo attende un exponential backoff e ritenta l'operazione di prendere un job da zero.
- seleziona il primo job con status "pending" e lo modifica in "in_progress" aggiungendo anche il lockedUntil usando CAS.
  - Se il file è stato modificato da un altro nodo, il nodo ritenta l'operazione da zero

== Complete job
Quando un nodo completa un job:
- legge il file corrispondente
- Rimuove il job completato e aggiorna il file su S3 con CAS. Se il file è stato modificato da un altro nodo, il nodo ritenta l'operazione da zero.
Quando un nodo vuole rimuovere l'ultimo job da un file, invece di rimuovere il job, elimina il file da S3 con CAS.

== Split
Se un nodo incontra un file con `status: "preparing_split"` i cui job sono tutti in status "pending", allora il nodo avvia una procedura di split:
1. Sceglie uno UUID a caso per il nuovo file
2. Modifica il file originale con CAS inserendo un lock che indica che il file è in fase di split, il nome del nuovo file e l'expire time del lock (`status: "splitting", splitInto: "<UUID>", lockedUntil: "<now+splitTimeout>"`). Se il file è stato modificato da un altro nodo, exponential backoff, poi riparte la procedura di split da zero, includendo la valutazione del se splittare o no.
3. Prende la prima metà dei job e li scrive in un nuovo file con nome `<UUID>.json` con `splitFrom: "<originalFile>"` e `status: "splitting"` e `lockedUntil: <now+splitTimeout>`. Se il file è già stato creato da un altro nodo, il nodo interrompe la procedura di split.
4. Sostituisce il file originale con CAS rimuovendo i job spostati e rimuovendo il lock di split (`status: "active", splitInto: unset, lockedUntil: unset`). Se il file è già stato modificato da un altro nodo, il nodo interrompe la procedura di split.
5. Modifica il nuovo file con CAS impostando `status: "active"` e rimuovendo `splitFrom` e `lockedUntil`.
Se è passato metà del tempo di splitTimeout, allora il nodo estende il lock di split su entrambi i file (se non è ancora stato creato il nuovo file, lo estende solo sul primo file) utilizzando CAS. Se il file è già stato modificato da un altro nodo, il nodo interrompe la procedura di split.


=== Failure recovery
Se un nodo incontra un file con `status: "splitting"` e `lockedUntil` è scaduto, allora il nodo avvia una procedura di recovery:
- Legge il file originale (quello con `splitInto`) e controlla se è in `status: "splitting"` e se `lockedUntil` è scaduto.
  - Se sì:
    - Modifica il file originale con CAS impostando `lockedUntil` a `<now+splitTimeout>` per acquisire il lock di split. Se il file è già stato modificato da un altro nodo, interrompe la procedura di recovery.
    - Legge il nuovo file
      - Se il file non esiste, riprende l'operazione di split dal punto 3.
      - Se il file esiste, riprende l'operazione di split dal punto 4.
  - Se no:
    - Legge il nuovo file (quello con `splitFrom`) e controlla se è in `status: "splitting"` e se `lockedUntil` è scaduto.
      - Se sì, riprende la procedura di split dal punto 5.
      - Se no, interrompe la procedura di recovery.
