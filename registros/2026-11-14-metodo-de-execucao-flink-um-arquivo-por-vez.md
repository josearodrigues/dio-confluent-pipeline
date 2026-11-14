# Método de execução do Passo 7: um arquivo por vez

Data: 14/11/2026

## Decisão de trabalho

O Passo 7 será conduzido pelo método incremental já utilizado no projeto privado de estudos: atacar uma unidade de cada vez, revisar, validar, executar, observar e só então avançar. O objetivo é concluir a pipeline com entendimento e evidências, não apenas submeter todos os statements em lote.

## Ciclo obrigatório por unidade

1. Ler o arquivo ativo no GitHub e a documentação técnica pertinente.
2. Consultar o equivalente no projeto privado de estudos, se existir, e comparar as versões.
3. Separar o que é comando/SQL literal do mentor, o que é adaptação do projeto e o que é análise independente.
4. Explicar objetivo, dependências, pré-condições, efeitos e critérios de validação.
5. Propor somente correções necessárias, com diff e justificativa; preservar o que já funciona.
6. Após sincronização pelo usuário, validar localmente a sintaxe e revisar a configuração sem expor segredos.
7. Obter autorização explícita antes de qualquer execução com impacto em Neon/Confluent ou custo.
8. Executar uma unidade por vez; analisar saída, estado remoto e evidência antes de continuar.
9. Registrar o estado real. Preparado não significa executado; statement criado não significa fluxo funcional; execução observada não significa homologação ponta a ponta.

## Situação ao abrir este registro

- Snapshot CDC: homologado, 2.760/2.760 eventos `op=r`.
- CDC incremental oficial: homologado, INSERT/UPDATE/DELETE `op=c/u/d`, offsets 231–233, `snapshot=false`.
- Evolução adicional do subject CDC real: homologada, versão 1 → 2, compatibilidade BACKWARD e evento `channel` observado.
- Flink: revisão/execução operacional ainda pendente; nenhuma execução do Passo 7 está autorizada por este registro.
- Pipeline ponta a ponta e emissão funcional de alerta: ainda não homologadas.

## Primeira unidade

Começar pela revisão do orquestrador `flink/scripts/run.sh`, identificando exatamente os arquivos SQL, a ordem de submissão, os efeitos externos e as proteções contra duplicação. Em seguida, revisar cada SQL individualmente e comparar com a resolução do mentor e com o projeto de estudos.

O orquestrador não deve ser executado como primeiro passo: ele pode criar compute pool e submeter vários statements. Primeiro compreender e revisar as unidades que ele encadeia.

## Limites

- Não repetir snapshot, CDC incremental ou evolução real do schema já homologados.
- Não presumir estado atual dos recursos Cloud; consultar antes de qualquer ação operacional.
- Não criar recursos, statements, service accounts, API keys ou ACLs sem necessidade comprovada e autorização.
- Não completar trechos truncados da transcrição do mentor por suposição.
- Em caso de falha, parar e diagnosticar antes de repetir ou avançar.

## Fonte de continuidade

O roteiro operacional detalhado permanece em `docs/plano-homologacao.md`; a rastreabilidade entre demonstração e implementação permanece em `docs/matriz-rastreabilidade-demo-mentor.md`. Este registro documenta a decisão de método e o ponto de retomada, sem substituir esses documentos.


## Atualização: preflight global do orquestrador (14/11/2026)

- Na revisão estática do flink/scripts/run.sh, foi identificado que a checagem de nomes ocorria apenas imediatamente antes de cada submissão. Um nome ocupado mais adiante poderia deixar a sequência parcialmente submetida.
- Correção mínima publicada: antes de qualquer submissão, consultar os statements do pool e verificar os cinco nomes planejados. Se algum já existir, abortar o preflight antes da primeira criação. A proteção individual de submit() foi mantida.
- O preflight não exclui, altera nem sobrescreve statements existentes. Ele apenas bloqueia a sequência quando detecta colisão.
- Comparação prévia com o script do projeto de estudos já realizada na revisão anterior; a versão de estudos continua não sendo referência para copiar, pois contém exclusão forçada de statements.
- Commit público do script: 4dfe1228f91f4cbd383c041cdbfcba9dabe3a58f.
- Plano atualizado em commit subsequente. A validação local pelo usuário ainda está pendente: sincronizar, revisar diff e executar bash -n flink/scripts/run.sh.
- Nenhum script foi executado; nenhuma operação Confluent Cloud/Neon, renomeação de pool ou submissão de statement foi realizada.
- Próxima unidade após validação local: revisar individualmente flink/sql/00-startup-transactions.sql, sem executar ainda.
