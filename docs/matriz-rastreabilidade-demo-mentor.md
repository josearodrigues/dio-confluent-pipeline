# Rastreabilidade da demonstração do mentor

Este documento relaciona as etapas observáveis da demonstração de referência aos artefatos correspondentes neste repositório. A matriz descreve o escopo implementado nos arquivos versionados; **não comprova que os recursos tenham sido provisionados ou que as etapas tenham sido executadas na Confluent Cloud**.

## Matriz de rastreabilidade

| Etapa da demonstração | Artefato do projeto | Papel no fluxo |
|---|---|---|
| Passo 3 — PostgreSQL como origem CDC | `database/init/` e `infrastructure/connectors/cdc/connector.json` | Prepara o schema/dataset e configura a origem PostgreSQL para captura CDC. |
| Passo 4 — Conector CDC | `infrastructure/connectors/cdc/connector.json` | Inclui customers, accounts, cards e transactions na captura CDC e publica os tópicos Kafka. |
| Passo 5 — Snapshot e CDC incremental | `operations/verify-cdc-snapshot.sh` e `operations/verify-cdc-incremental.sh` | Valida o snapshot inicial e, em seguida, observa INSERT, UPDATE e DELETE (eventos Debezium `c`, `u`, `d`) em tempo real. |
| Passo 6 — Contrato e compatibilidade Avro | `infrastructure/schema-registry.sh`, `schemas/transaction-v1.avsc`, `schemas/transaction-v2.avsc` | Registra v1, testa evolução compatível com campo opcional/default e confirma rejeição de campo obrigatório sem default no subject isolado `contract.transactions-value`. |
| Extensão do projeto — Evolução do schema CDC real | `infrastructure/schema-evolution.sh` | Após o CDC incremental, adiciona `channel` à tabela `payments.transactions` e valida a publicação da nova versão no subject real `payments.transactions-value`. Não é o teste isolado do Passo 6 do mentor. |
| Passo 7 — Startup offset e chaves Flink | `flink/sql/00-startup-transactions.sql`, `flink/sql/00-pk-accounts.sql`, `flink/sql/00-pk-transactions.sql` | Configura transactions para iniciar em `latest-offset` após o snapshot e declara as chaves `NOT ENFORCED` necessárias ao processamento. |
| Passo 7 — Temporal Join | `flink/sql/01-enrichment.sql` | Enriquece cada transação com a versão de accounts válida no rowtime do evento. |
| Passo 7 — Entrada append-only | `flink/sql/02-append-only.sql` | Configura o changelog append para a etapa de reconhecimento de padrão. |
| Passo 7 — Detecção de padrão temporal | `flink/sql/04-fraud-detection.sql` | Usa MATCH_RECOGNIZE para identificar três ou mais transações positivas por cartão em até 60 segundos de rowtime Kafka. |
| Passo 7 — Cenário positivo e saída | `database/seed/simulate-events.sql` e tópico Kafka `fraud_alerts` | Insere quatro transações positivas para o mesmo cartão e publica o resultado da detecção no tópico de alertas. O fluxo principal não inclui PostgreSQL Sink. |

## Ordem de execução no projeto e diferença em relação à demo

A sequência conceitual da demonstração é: **Passo 5 — CDC incremental; Passo 6 — evolução compatível/incompatível de contrato Avro em subject isolado; Passo 7 — Flink**.

Há duas diferenças de implementação que precisam permanecer explícitas:

1. Neste repositório, `infrastructure/schema-registry.sh` provisiona o Schema Registry **e executa** o teste de contrato isolado na etapa de preparação, antes do conector CDC. Portanto, o teste correspondente ao Passo 6 do mentor é executado antecipadamente no roteiro atual; não deve ser repetido depois, pois o script interrompe se o subject já existir.
2. `infrastructure/schema-evolution.sh` é uma validação adicional deste projeto: evolui o subject real produzido pelo CDC. Ela é executada depois do CDC incremental e antes do Flink, mas **não substitui nem corresponde ao teste isolado do Passo 6**.

Assim, a ordem operacional atual após o snapshot é: CDC incremental (Passo 5) → evolução adicional do subject CDC real → Flink (Passo 7). O teste isolado do contrato (Passo 6) já foi executado antes do CDC pela organização atual do script de Schema Registry.
## Adaptações em relação à demonstração

- O projeto utiliza o domínio `payments` e os nomes de tabelas `customers`, `accounts`, `cards` e `transactions`.
- A tabela auxiliar `merchants` não integra a publication CDC.
- O teste isolado de compatibilidade Avro e a evolução adicional do subject CDC real são validações distintas, com subjects e objetivos diferentes.
- Após validar o snapshot, transactions é configurada para iniciar em `latest-offset`, evitando que a carga histórica seja interpretada como novos eventos pelo fluxo de fraude. A dimensão accounts permanece disponível para o Temporal Join.
- A saída principal é o tópico Kafka `fraud_alerts`; persistência em PostgreSQL não faz parte do escopo principal.
- O cenário positivo usa INSERTs novos. Isso demonstra a regra pretendida, mas não elimina as limitações gerais da transformação append.

## Semântica e limites de interpretação

- A janela de 60 segundos usa o timestamp de evento Kafka (`$rowtime`), não o campo de negócio `created_at`.
- As chaves declaradas como `NOT ENFORCED` são metadados para o Flink; não verificam unicidade na origem.
- O modo append não equivale a filtrar exclusivamente operações Debezium `op=c`. Conforme a configuração documentada no SQL, eventos `c`, `r` e `u` são tratados como novas linhas e `d` é ignorado. Portanto, atualizações podem contribuir para a contagem do padrão.
- A regra é didática e não representa reconciliação completa do estado das entidades, deduplicação ou um motor de risco de produção.
- A presença dos scripts e SQL nesta matriz indica rastreabilidade documental, não execução bem-sucedida. O estado de cada etapa deve ser registrado somente com evidência produzida e verificada durante a homologação.

## Critério para registrar conclusão

Uma etapa só deve ser marcada como executada ou homologada quando houver evidência correspondente em `evidence/`, com contexto suficiente para identificar o comando/ação, o resultado observado e as limitações. Scripts preparados, instruções e resultados históricos de ambientes já excluídos não comprovam a execução no ambiente atual.

## Investigação complementar — disparo do padrão de fraude no vídeo

**Registro de 14/11/2026.** Esta seção separa o que foi efetivamente observado no trecho disponibilizado pelo usuário, as hipóteses de reconstrução e o que permanece sem comprovação. O mentor não disponibilizou o repositório/código-fonte; os comandos abaixo não são transcrição literal.

### Trecho observável do vídeo

O trecho recuperado mostra a seguinte sequência:

1. Um loop Bash executa três comandos docker run --rm postgres:16-alpine psql "$PGURL" -c "INSERT ..." em sequência.
2. O INSERT lista explicitamente as colunas transaction_id, account_id, card_id, merchant_id, channel, amount, mcc, uf_transacao e created_at.
3. O SELECT visível usa gen_random_uuid(), referencia payments.cards c e uma relação/alias m, e seleciona pelo menos c.account_id, c.card_id e m.merchant_id.
4. O texto do mentor descreve os três eventos como transações do mesmo cartão.
5. Em seguida, o mentor executa um quarto INSERT, descrito como uma transação “inocente”, para avançar a watermark.
6. Após aproximadamente 1–2 minutos, a consulta ao tópico desafio.fraud.detected deveria mostrar um alerta novo com txn_count = 3.

O material recuperado está cortado. Não foi possível observar integralmente o FROM, os JOIN, os filtros, os valores de todas as colunas, o comando completo da quarta transação nem a consulta de validação. Não atribuir ao mentor detalhes que não aparecem no trecho.

### Hipótese de reconstrução apresentada para os três INSERTs

O usuário propôs, como palpite e não como transcrição:

~~~bash
for i in 1 2 3; do
  docker run --rm postgres:16-alpine psql "$PGURL" -c "INSERT INTO payments.transactions (transaction_id, account_id, card_id, merchant_id, channel, amount, mcc, uf_transacao, created_at) SELECT gen_random_uuid(), c.account_id, c.card_id, m.merchant_id, 'POS', 100.00, m.mcc, 'SP', now() FROM payments.cards c, payments.merchants m ORDER BY c.card_id, m.merchant_id LIMIT 1;"
done
~~~

A hipótese seleciona repetidamente a combinação de menor card_id e menor merchant_id, mantendo o mesmo cartão nos três comandos, desde que os dados não mudem entre as execuções. Isso é coerente com a partição PARTITION BY card_id do reconhecimento de padrão. Os valores 'POS', 100.00, 'SP', o uso de m.mcc, now(), o produto cartesiano, o ORDER BY e o LIMIT 1 são suposições do usuário; não foram confirmados no vídeo.

### Hipótese de reconstrução apresentada para o quarto INSERT

O usuário propôs:

~~~bash
docker run --rm postgres:16-alpine psql "$PGURL" -c "INSERT INTO payments.transactions (transaction_id, account_id, card_id, merchant_id, channel, amount, mcc, uf_transacao, created_at) SELECT gen_random_uuid(), c.account_id, c.card_id, m.merchant_id, 'POS', 10.00, m.mcc, 'SP', now() FROM payments.cards c, payments.merchants m ORDER BY c.card_id DESC, m.merchant_id LIMIT 1;"
~~~

A intenção é selecionar o maior card_id, diferente do menor selecionado nos três primeiros INSERTs, e produzir um evento adicional. Com os dados atuais do projeto, há múltiplos cartões e merchants, mas essa consulta ainda é somente uma hipótese. Os valores e a razão exata pela qual o mentor chama o evento de “inocente” são desconhecidos.

### Confronto com o schema PostgreSQL do repositório público

O arquivo ativo database/init/01-database.sql define:

- transactions.transaction_id como BIGINT GENERATED BY DEFAULT AS IDENTITY, não UUID;
- colunas card_id, account_id, merchant_id, amount, currency, status e created_at;
- não define channel, mcc nem uf_transacao;
- merchants contém merchant_id, name, category e city; não contém mcc;
- merchants existe no PostgreSQL, mas fica fora da publication CDC.

Logo, os palpites do vídeo não podem ser executados sem adaptação contra o schema atual do repositório público. Em particular, gen_random_uuid() não é compatível com a coluna identity BIGINT, e as três colunas adicionais não existem no DDL atual.

Uma adaptação conceitual ao schema público poderia omitir transaction_id para deixar a identity gerar o valor e usar somente card_id, account_id, merchant_id, amount, currency, status e created_at. Não foi criada nem executada; não é transcrição do mentor nem alteração proposta ao arquivo de seed existente.

### Semântica do MATCH_RECOGNIZE e ressalva sobre a quarta transação

O SQL público flink/sql/04-fraud-detection.sql usa:

- PARTITION BY card_id;
- ORDER BY $rowtime;
- PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND;
- DEFINE T AS T.amount > 0.

Consequências:

- O critério explícito de elegibilidade de T é amount > 0. O SQL não classifica inocência por merchant, MCC, canal ou UF.
- Um quarto evento positivo em outro cartão não pertence à mesma partição das três primeiras transações, mas ainda pode ser um evento elegível em sua própria partição.
- Portanto, não se pode afirmar que a quarta transação fique fora do padrão apenas por ser chamada de “inocente” ou por ter outro card_id.
- O quantificador reluctant T{3,}? é compatível com a intenção de encerrar a correspondência no menor número de eventos que satisfaz o padrão; o comportamento concreto ainda depende do processamento temporal e da chegada dos eventos.

### Watermark: o que sabemos e o que falta verificar

O SQL de enrichment flink/sql/01-enrichment.sql usa o $rowtime de payments.transactions como tempo do evento e a saída enriquecida preserva o timestamp Kafka em uma coluna metadata rowtime. O SQL de fraude ordena por $rowtime, não por created_at.

O mentor atribui ao quarto INSERT o papel de avançar a watermark e liberar a emissão do alerta. Isso é uma explicação observada no vídeo, mas não está demonstrado pelos trechos de código disponíveis.

**Não é correto garantir que um evento de outro cartão avance a watermark global.** O resultado depende da atribuição de timestamps/watermarks na fonte, da distribuição entre partições Kafka, da propagação de watermarks e de eventual ociosidade de partições. O fato de o evento estar em outro card_id não permite concluir em qual partição Kafka ele será escrito nem qual watermark será emitida.

O plano atual aplica sql.tables.scan.idle-timeout aos statements contínuos, mas isso, isoladamente, não comprova toda a estratégia de watermark nem garante a emissão após o quarto INSERT.

### Estado desta investigação e próximo critério

- **Observado no vídeo:** três INSERTs em sequência para o mesmo cartão; quarto INSERT descrito como inocente e usado para avançar a watermark; consulta posterior espera alerta com txn_count = 3.
- **Hipótese do usuário:** comandos com gen_random_uuid(), payments.cards × payments.merchants, ORDER BY ... LIMIT 1, valores POS/amount/UF e seleção de outro cartão no quarto evento.
- **Conferido estaticamente:** incompatibilidades entre os campos/tipos do palpite e o DDL PostgreSQL público; condição amount > 0, partição por cartão, padrão reluctant e uso de $rowtime no SQL Flink público.
- **Não executado nesta investigação:** nenhum INSERT, SQL Flink, statement, consulta à Confluent Cloud ou alteração de ambiente. Neon e Confluent permanecem zerados conforme a confirmação mais recente do usuário.
- **Pendente:** antes de alterar o cenário de fraude, revisar a definição efetiva das tabelas Flink/timestamps, a estratégia de watermark e o particionamento do tópico de entrada; depois, em ambiente reconstruído e com autorização, testar separadamente o padrão de três eventos e o mecanismo de avanço da watermark, registrando os resultados observados.

Não substituir o cenário positivo versionado database/seed/simulate-events.sql por estes palpites. O arquivo atual usa quatro INSERTs positivos em uma única instrução para o mesmo cartão e continua sendo uma implementação distinta da sequência descrita no vídeo.


## Investigação do requisito de confiabilidade e destino dos alertas (14/11/2026)

Esta seção registra a interpretação documental feita a partir dos materiais do desafio e da demonstração disponíveis até esta data. Não representa homologação de uma implementação.

### Materiais examinados
- `4.0.0.3-1-Desafio Final - Pipeline de pagamentos (entrega de exemplo).pdf`.
- `4.0.0.3-Entendendo o Desafio.md`.
- `4.0.0.2-Resolução do desafio de projeto.srt` (transcrição automática; sujeita a erros de reconhecimento).
- `4.0.0.2-desafio-final-demo.md` (roteiro auxiliar derivado da demonstração; não equivale ao vídeo nem necessariamente ao README oficial citado pelo enunciado).

### O que os materiais do desafio permitem afirmar
- A entrega é organizada em quatro pilares: observabilidade, segurança, custos e confiabilidade.
- Observabilidade pede evidência de pico de `received_bytes` e lag por grupo consumidor.
- Segurança pede relacionar Service Accounts, ACLs e justificativas.
- Custos pede consulta de Billing do período e duas alavancas concretas de redução.
- Confiabilidade pede explicitar garantias trecho a trecho, com atenção ao efeito produzido pelo consumidor.
- O exemplo de entrega resume o ponto crítico como “consumer → efeito” e esclarece que a idempotência desse efeito é responsabilidade do código da aplicação, não uma configuração automática do Kafka.
- O material recuperado não especifica que o efeito precise ser persistido em PostgreSQL. A expressão “consumer → efeito” não determina, por si só, o uso de PostgreSQL Sink.
- O PDF remete a uma tabela de garantias trecho a trecho no “README do desafio”. Esse README específico ainda não foi localizado. Não confundir com o README deste repositório.

### O que a demonstração do mentor efetivamente apresenta nos materiais disponíveis
1. Criação do Environment e cluster Basic.
2. Criação/preparação do PostgreSQL Neon e dataset sintético.
3. Criação/configuração do CDC Source V2 e observação de snapshot/eventos incrementais.
4. Teste de compatibilidade/evolução de contrato Avro.
5. Flink: chave primária, Temporal Join, append e MATCH_RECOGNIZE.
6. Cenário de fraude: três INSERTs positivos do mesmo cartão e uma quarta transação inocente para avançar o watermark; consulta de verificação do resultado.
7. Observabilidade, segurança e Billing.
8. Teardown.

O roteiro descreve como saída da detecção o tópico Kafka de alertas e uma consulta a esse tópico. A transcrição contém uma fala do mentor, por volta de 06:49, dizendo que o desafio seria criar conector, processar e “entregar de volta o dado para o postgres”. Entretanto, nos materiais de transcrição/roteiro disponíveis não aparece uma etapa concreta de criação, configuração, execução ou validação de PostgreSQL Sink. A transcrição automática pode omitir detalhes do vídeo; portanto, a conclusão correta é “Sink não demonstrado nos materiais examinados”, e não “o mentor definitivamente não usou Sink”.

### Conclusão provisória
- O fluxo mínimo demonstrado termina em alertas no Kafka.
- A persistência em PostgreSQL permanece ambígua e não pode ser tratada como requisito confirmado.
- O PostgreSQL Sink existente no projeto privado de estudos deve continuar classificado como extensão até que o enunciado original ou o README citado demonstre exigência explícita.
- Não alterar os projetos nem implementar consumidor/Sink adicional com base apenas na fala isolada.

### Pendência para a próxima sessão
Localizar e examinar o README original do desafio que contém a tabela de garantias trecho a trecho. O usuário informou que adicionou outros arquivos do desafio com nomes iniciados por `4.0...`; esses arquivos ainda precisam ser inventariados e lidos antes de fechar a interpretação. Se a fonte não estiver disponível, registrar a lacuna e não preencher por inferência.

### Próxima sequência acordada
1. Inventariar e ler os novos materiais `4.0...`.
2. Localizar a tabela/README original e interpretar as garantias por trecho.
3. Consolidar o que foi pedido, o que o mentor demonstrou e o que permanece não demonstrado.
4. Só depois cruzar esses resultados com os projetos público e privado e decidir, uma etapa por vez, se há mudanças necessárias.


## Complemento: escopo end-to-end e destino dos dados (14/11/2026)

A análise dos materiais adicionais `4.0.0.1` e `4.0.0.4` refinou a conclusão provisória anterior.

### Novos materiais examinados
- `4.0.0.1-Desafio de projeto- Construindo um pipeline rela-time end-to-end com Confluent Cloud.srt`: transcrição automática da apresentação do desafio.
- `4.0.0.4-Informações.md`: descrição resumida oficial do desafio.
- `4.0.0.0-Confluent Cloud- Desafio Final com Pipeline Real-Time End-to-End.png`: arquivo localizado e disponibilizado em pré-visualização; seu conteúdo visual ainda não foi interpretado tecnicamente nesta revisão.
- Inventário da pasta Dropbox `Bootcamp IBM Confluent` concluído: listagem direta retornou `has_more=false`; não há README entre os arquivos atualmente nessa pasta.

### O que os novos materiais acrescentam
A transcrição introdutória do desafio afirma:
- O pipeline deve estar funcional.
- A entrega deve ser publicada em repositório público GitHub.
- O repositório deve conter código, SQL, scripts e processo completo, com README bem documentado.
- Cada camada deve ter evidências, outputs e comandos.
- Deve haver processo de setup e teardown para permitir reprodução por terceiros.
- O fluxo proposto é trazer tabelas do PostgreSQL por CDC, processá-las, aplicar regras de fraude e produzir uma saída.
- Sobre o destino, a fala é explícita: devolver os dados ao PostgreSQL **ou a outro lugar**, sendo PostgreSQL a recomendação do mentor.

O arquivo `4.0.0.4-Informações.md` descreve cinco camadas, da fundação do ambiente à operação com métricas e custo, e reforça que o README deve documentar cada camada com evidência de execução.

### Ajuste de interpretação
- O desafio não se limita a produzir um alerta dentro do Kafka: a apresentação introdutória descreve uma etapa de saída/entrega para um destino consumidor.
- PostgreSQL é destino recomendado, mas não obrigatório pela fala transcrita: outro destino é expressamente permitido.
- Os materiais ainda não identificam de forma conclusiva o mecanismo dessa entrega (PostgreSQL Sink gerenciado, consumidor de aplicação, outro conector ou outro destino).
- A transcrição da resolução do mentor/roteiro operacional disponível continua sem mostrar os passos concretos de configuração e validação desse destino. Portanto, a formulação correta é: **o destino externo faz parte do escopo pretendido do desafio; o mecanismo e a implementação demonstrada permanecem não esclarecidos nos materiais operacionais examinados**.
- A afirmação anterior de que “o fluxo mínimo do desafio termina em Kafka” deve ser considerada corrigida: a demonstração operacional recuperada valida a detecção/alerta no Kafka, mas a introdução do desafio descreve também a entrega do resultado a um destino.
- Os quatro pilares (observabilidade, segurança, custos e confiabilidade) são critérios operacionais/de avaliação; as cinco camadas mencionadas em `4.0.0.4` são a organização do pipeline/entrega. Não confundir as duas classificações.

### README de garantias
A listagem completa da pasta Dropbox não encontrou o README citado pelo PDF. Busca pública no GitHub localizou repositório de terceiro `AlsS99/desafioDIOcomIBM` com uma tabela “Confiabilidade” por trecho, incluindo “Consumer → efeito”, entrega at-least-once e idempotência implementada na aplicação. É um trabalho de participante, não fonte oficial; serve apenas como referência comparativa e não comprova o conteúdo do README original do desafio. Não copiar nem tratar como requisito oficial.
A tabela original continua não localizada.

### Próxima etapa
1. Identificar a origem/link do README oficial mencionado pelo PDF (possivelmente no ambiente do curso/DIO ou em material ainda não fornecido).
2. Interpretar a imagem `4.0.0.0` e verificar se contém diagrama/indicação explícita do destino.
3. Examinar a demonstração operacional completa para procurar a etapa de entrega do alerta ao destino.
4. Só então definir se a implementação deve usar Sink gerenciado, consumidor de aplicação ou outro mecanismo.

Nenhuma implementação foi alterada e nenhuma operação Confluent/Neon foi executada nesta revisão.


## Fechamento do inventário do acervo Dropbox (14/11/2026)

Após confirmação do usuário de que não há outras fontes além do Dropbox, o acervo disponível deve ser tratado como o conjunto completo de materiais fornecidos para esta investigação. A listagem recursiva da pasta `Bootcamp IBM Confluent` foi concluída com `has_more=false`.

### Inventário dos arquivos 4.0 disponíveis
- `4.0.0.0-Confluent Cloud- Desafio Final com Pipeline Real-Time End-to-End.png` — imagem localizada e exibida pela pré-visualização do Dropbox; o conteúdo interno do diagrama não foi extraído/interpretado nesta sessão.
- `4.0.0.1-Desafio de projeto- Construindo um pipeline rela-time end-to-end com Confluent Cloud.srt` — transcrição automática da apresentação do desafio.
- `4.0.0.2-Resolução do desafio de projeto.srt` — transcrição automática da demonstração de resolução.
- `4.0.0.2-desafio-final-demo.md` — roteiro textual auxiliar da demonstração.
- `4.0.0.3-1-Desafio Final - Pipeline de pagamentos (entrega de exemplo).pdf` — exemplo de entrega e critérios dos quatro pilares.
- `4.0.0.3-Entendendo o Desafio.md` — descrição/explicação do desafio.
- `4.0.0.4-Informações.md` — descrição curta do desafio.

Não há outros arquivos 4.0 na listagem atual da pasta. Também existem materiais de apoio das aulas 1.x, 2.x e 3.x; eles não foram todos reexaminados nesta etapa, pois a investigação atual está delimitada à interpretação dos materiais finais 4.0. Não afirmar que esses materiais de aula foram analisados integralmente.

### Leitura consolidada e limites
- `4.0.0.4` declara: pipeline de pagamentos em tempo real; PostgreSQL → CDC → Flink SQL para enriquecer transações e marcar suspeitas → entrega do resultado a quem vai consumir; cinco camadas, da fundação à operação; README público com evidência de execução de cada camada.
- `4.0.0.1.srt`, em 00:00:25–00:00:58, pede pipeline funcional, repositório público, código/SQL/scripts, README com evidências/outputs/comandos e setup/teardown reproduzíveis.
- `4.0.0.1.srt`, em 00:01:41–00:02:07, descreve saída dos dados processados de volta ao PostgreSQL **ou para outro lugar**, recomendando PostgreSQL. Como é SRT automático, a redação exata deve ser conferida contra o áudio/vídeo se necessário; ainda assim, a ideia é coerente com `4.0.0.4` (“entrega o resultado para quem vai consumir”).
- Portanto, o requisito conceitual de saída/entrega ao consumidor está confirmado; PostgreSQL é recomendado, não obrigatório. O mecanismo concreto (PostgreSQL Sink, consumidor de aplicação ou outro) não está identificado nos documentos finais disponíveis.
- Os quatro pilares (observabilidade, segurança, custos, confiabilidade) do exemplo são dimensões de operação/avaliação; “cinco camadas” é a organização geral do pipeline mencionada na descrição. Não misturar essas duas estruturas.
- A tabela de garantias citada pelo PDF como estando no README do desafio não aparece nos arquivos atualmente disponíveis no Dropbox. O README do projeto de outro participante encontrado por busca pública não é oficial e não substitui essa fonte. Assim, a tabela original deve ser registrada como **não disponível no acervo**, não como arquivo a procurar indefinidamente.

### Próximo encaminhamento
O material disponível é suficiente para estabelecer o requisito conceitual end-to-end e a necessidade de uma saída consumível. Não é suficiente para exigir especificamente PostgreSQL Sink nem para reproduzir uma tabela oficial de garantias trecho a trecho. A análise deve avançar usando essa distinção, sem pedir ao usuário arquivos inexistentes nem buscar uma fonte alternativa que ele já informou não possuir.
A próxima etapa, quando autorizada, será confrontar esses requisitos com a demonstração do mentor e os dois projetos, identificando a solução de saída efetivamente implementada e as lacunas de confiabilidade. Não alterar implementação antes desse confronto.
