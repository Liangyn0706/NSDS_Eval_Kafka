# Kafka Eval 2026 — ambiente locale

Prerequisiti: Java 17+, Maven e Python 3 (già disponibili sulla macchina configurata).
Il broker usa Kafka 4.4.0, come `kafka-clients` nel `pom.xml`, in modalità KRaft,
secondo il [quickstart ufficiale](https://kafka.apache.org/quickstart/).
Non serve ZooKeeper. Il primo avvio scarica Kafka e verifica il checksum SHA-512.

## Avvio con Play

In VS Code aprire **Esegui e debug**, selezionare **Producer (Kafka locale)** e premere
Play/F5. La configurazione avvia prima il broker su `localhost:9092` e crea i topic
senza cancellare quelli esistenti. Le estensioni Java richieste sono già installate.
Usare la configurazione salvata per eseguire anche il passaggio di avvio del broker.
Per il test automatico usare **Terminale → Esegui attività → Kafka: test Producer**.

In IntelliJ importare il progetto Maven e selezionare **Producer (Kafka locale)**.
Le configurazioni condivise in `.run/` includono il passaggio **Kafka locale** prima
dell'esecuzione (richiede il supporto Shell Script di IntelliJ).
Per il test automatico selezionare **Test Kafka (Producer reale)** e premere Play.

Sono disponibili anche **Monitor (stage 1)** con argomenti `1 monitor-local` e
**Reactor (stage 2)** con argomenti `2 reactor-local`.
`ConsumersXX.java` contiene ancora i TODO e cicli vuoti: questi processi non
elaborano messaggi e consumano CPU finché non si completa la logica. Il test
infrastrutturale usa il Producer esistente e un consumer di verifica separato.

## Comandi

```bash
bash scripts/kafka-local.sh start   # installa se necessario, avvia e prepara i topic
bash scripts/kafka-local.sh status  # descrive topic e partizioni
bash scripts/test-kafka.sh          # compila e verifica messaggi reali del Producer
bash scripts/kafka-local.sh stop    # arresta solo il broker del progetto
```

Il test verifica chiavi String e valori Integer su entrambi i topic di ingresso,
lascia alcuni messaggi di esempio e arresta il Producer di verifica.
Il broker resta attivo anche dopo Stop nell'IDE; usare il comando `stop` per fermarlo.

| Topic | Partizioni | Repliche |
| --- | --- | --- |
| temperature | 4 | 1 |
| humidity | 4 | 1 |
| alerts | 1 | 1 |
| processedAlerts | 1 | 1 |

Questa è una configurazione locale per le prove; il numero di partizioni dei topic
di uscita potrà essere adattato alla soluzione dell'esercizio.
Download, dati persistenti, PID e log sono in `.local/`, esclusa da Git.
Log del broker: `.local/logs/server.out`.
Non eseguire `TopicManager` sui topic da conservare: elimina il topic se esiste.
