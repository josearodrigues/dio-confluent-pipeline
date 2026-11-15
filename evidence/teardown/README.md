# Evidências de teardown

Esta pasta reúne os relatórios gerados pelas rotinas de teardown do laboratório.

## Conteúdo

- Relatórios de execução do teardown, com data/hora no nome do arquivo.
- Resumo do resultado por etapa, distinguindo solicitação, execução, verificação posterior, bloqueios e falhas.
- Referências aos logs detalhados, quando aplicável.

## Como interpretar

A presença de um relatório comprova apenas que a rotina registrou aquela execução. Consulte o conteúdo para identificar o que foi solicitado, o que foi efetivamente executado e o que foi confirmado por verificações posteriores. Uma etapa marcada como bloqueada ou com falha não deve ser considerada concluída.

Os relatórios não substituem a inspeção do estado atual dos recursos na Confluent Cloud ou no PostgreSQL/Neon. Não inclua credenciais, tokens, senhas ou valores de variáveis secretas nas evidências.

## Convenção

Os arquivos são mantidos com os nomes e timestamps produzidos pelos scripts. Não sobrescreva relatórios anteriores: cada execução deve preservar seu próprio registro.

