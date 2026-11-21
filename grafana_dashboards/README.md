# Dashboard Grafana di multiDAQ

Tre dashboard nel formato JSON classico di Grafana, esportate con "Export for sharing externally": funzionano su qualsiasi versione di Grafana.

- `multidaq_overview.json`: stato generale del sistema.
- `multidaq_acquisition.json`: metriche dell'acquisizione.
- `multidaq_client_detail.json`: dettaglio di un client (HV, monitoraggio).

## Importazione

1. In Grafana: Dashboards → New → Import → Upload dashboard JSON file.
2. Alla voce `DS_PROMETHEUS` scegliere la data source Prometheus dell'installazione.
3. Importare.

La data source deve raccogliere le metriche dall'exporter Prometheus del server multiDAQ (metriche `multidaq_*`).
Se si riesportano da Grafana, usare il formato classico con "Export for sharing externally", così il riferimento alla data source resta `${DS_PROMETHEUS}`.
