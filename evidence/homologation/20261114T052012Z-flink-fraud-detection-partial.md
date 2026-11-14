# Evidência — execução inicial do Flink e detecção de padrões

**Data:** 14/11/2026  
**Ambiente:** Environment `env-mn3vgw`; Kafka cluster/database `lkc-rrwvjjk`; compute pool `lfcp-2268w8o` (`desafio-pool`); AWS `us-east-2`.  
**Statement de detecção:** `payments-fraud-detection-test`, criado às 05:20:12 UTC.

## Execução observada

- `payments-enrichment-test`: criado anteriormente e observado em `RUNNING`.
- `payments-append-only-test`: observado em `COMPLETED`, com `Command completed successfully.`.
- `payments-fraud-detection-test`: criação aceita pela CLI e estado observado `RUNNING`. O SQL executado corresponde a `flink/sql/04-fraud-detection.sql`.
- `confluent flink materialized-table list` retornou lista vazia (`[]`). Isso não impediu a verificação posterior do catálogo SQL.
- `SHOW TABLES` no Flink SQL Workspace listou `enriched_payments`, `fraud_alerts` e as quatro tabelas CDC: `payments.accounts`, `payments.cards`, `payments.customers` e `payments.transactions`.
- `DESCRIBE EXTENDED fraud_alerts` mostrou:
  - `card_id INT NOT NULL`;
  - `first_txn TIMESTAMP_LTZ(3) NOT NULL`;
  - `last_txn TIMESTAMP_LTZ(3) NOT NULL`;
  - `txn_count BIGINT NOT NULL`;
  - `$rowtime TIMESTAMP_LTZ(3) ROWTIME NOT NULL`, metadata virtual, watermark `SOURCE_WATERMARK()`.
- A consulta resumida `SELECT COUNT(*), MIN(txn_count), MAX(txn_count) FROM fraud_alerts` retornou:
  - `total_alerts = 641`;
  - `min_transactions = 3`;
  - `max_transactions = 3`.
- A consulta gerou o aviso `AGGREGATION_WITHOUT_KEY`, pois é uma agregação não-windowed sem `GROUP BY`, concentrando estado em uma única instância. Foi recomendado interromper essa consulta exploratória após obter o resultado; não interromper os statements contínuos de enrichment/detecção.

## Interpretação e limites

**Comprovado nesta execução:** o statement de detecção foi aceito e permaneceu em execução; a tabela `fraud_alerts` existe no catálogo SQL; seu esquema expõe a coluna temporal; uma consulta resumida observou 641 linhas agregadas, todas com `txn_count=3`.

**Ainda não comprovado:** correlação dos 641 alertas com eventos de origem específicos; se os eventos vieram do cenário de seed ou do snapshot/CDC; se a janela de 60 segundos e a watermark produziram exatamente o comportamento pretendido; nem a publicação/consumo de mensagens no tópico Kafka de saída. A existência da tabela e a contagem agregada não homologam, isoladamente, a regra temporal ponta a ponta.

A consulta `SELECT * FROM fraud_alerts` foi considerada excessiva para inspeção interativa e não deve ser repetida sem limite. A agregação global foi exploratória, não um teste de desempenho.

**Classificação:** execução parcial observada; detecção funcional inicial observada; homologação ponta a ponta ainda pendente.

**Próximo passo:** antes de inserir novos eventos, verificar a origem e o conteúdo de amostras limitadas dos alertas e confrontá-los com transações de origem, sem repetir a agregação global nem despejar todas as linhas.
