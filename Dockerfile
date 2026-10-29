FROM busybox:1.36.1
COPY target/exporter-*.jar /exporter/ph-ee-kafka-exporter.jar
CMD ["cp", "/exporter/ph-ee-kafka-exporter.jar", "/exporters/"]
