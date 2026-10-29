## Purpose
This exporter component is in charge of exporting the Zeebe workflow engine's gathered events in JSON form into a Kafka topic. Once built, this component runs inside the Zeebe broker engine.

## Configuration
The following environment variables can be used to configure the exporter:

| Environment variable | Description |
| --- | --- |
| ZEEBE_KAFKAEXPORT_URL | Kafka broker URL |
| ZEEBE_KAFKAEXPORT_TOPIC | Kafka topic to export events to |
| ZEEBE_KAFKAEXPORT_TOPIC_PARTITIONS | Kafka topic partition count (used on auto topic creation) |
| ZEEBE_KAFKAEXPORT_TOPIC_REPLICATION_FACTOR | Kafka topic replication factor (used on auto topic creation) |
| ZEEBE_MSK_ENABLED | Enable MSK (AWS Managed Kafka) support |
```

## Build and image
`mvn verify` builds the shaded exporter JAR in `target/`. The `Dockerfile` packages it into a small image that holds
only the JAR at `/exporter/ph-ee-kafka-exporter.jar`; its default command copies the JAR to `/exporters/`.

The image is used as an init container of the Zeebe brokers, which share an `/exporters` volume with it and load the
JAR from there, so the brokers keep the stock `camunda/zeebe` image.

The build workflow tests every pull request and publishes `oneacrefund/ph-ee-kafka-exporter:<version>.<date>.<run>`
to ECR on pushes to `develop`, `main`, `release/*` and `hotfix/*`.

## Versioning
The version is the Zeebe version the exporter is built for, plus an OAF build number: `8.2.29-oaf.1`. Use it only
with brokers on the same Zeebe minor version. Bump `oaf.N` for each release, and reset it to `oaf.1` when moving to a
new Zeebe version. Image tags add the build date and CI run number, e.g. `8.2.29-oaf.1.2026.10.29.12`.
