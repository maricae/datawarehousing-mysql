# Apache Airflow Data Warehousing - MySQL to MySQL

Este projeto é a construção de um Data Warehouse (DW) para melhorar o desempenho de dashboards e consultas analíticas de uma empresa de logística. Dados de um Read Replic (MySQL) para outro banco MySQL (DW). Para a construção das pipelines foi utilizado o orquestrador Apache Spark.

## Tabelas
Foram 4 tabelas construídas no DW com base nos dados do Read Replic, são elas:

| Tabelas | Descrição |
| --- | --- |
| `creators` | Dados dos donos dos produtos |
| `orders` | Dados dos pedidos iniciados com seus respectivos status |
| `products` | Dados dos produtos cadastrados na empresa |
| `stock` | Histórico de entradas e saídas dos produtos |


## Etapas das DAGs
### 1. Query Execute
Executa a consulta no Read Replic.
### 4. Truncate Table
Limpa a tabela do DW para a entrada dos novos dados atualizados.
### 5. Insert Table
Insere os dados atualizados.
### 6. Email on Failed
Envia e-mail caso houver falha em algumas das etapas.

