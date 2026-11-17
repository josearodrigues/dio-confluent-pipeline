# Evidências

Este diretório reúne saídas geradas por execuções reais do laboratório, organizadas por finalidade.

## Diretórios

- `local/`: registros de preparação, consultas e operações no PostgreSQL, incluindo inicialização e limpeza.
- `cloud/`: registros do estado e da configuração dos recursos Confluent, incluindo Environment, Schema Registry, conectores CDC e Sink, e Flink.
- `homologation/`: evidências dos testes funcionais, CDC (snapshot e incremental), evolução de schema, execução Flink, alertas e consultas de destino.
- `observability/`: métricas, verificações de ACLs/RBAC, informações de Billing e análises de confiabilidade.
- `teardown/`: relatórios do encerramento e da limpeza dos recursos utilizados no laboratório.

Cada subdiretório possui um README próprio com sua finalidade e orientações sobre as evidências armazenadas.

## Cuidados

- Registrar somente saídas de execuções reais.
- Não criar arquivos fictícios para preencher esta pasta.
- Revisar os arquivos antes de versionar, removendo segredos, credenciais e dados pessoais.
- Preservar a distinção entre operações executadas, validações concluídas e atividades parciais ou não realizadas.
