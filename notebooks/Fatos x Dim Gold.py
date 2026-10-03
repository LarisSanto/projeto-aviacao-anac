# Databricks notebook source
# MAGIC %md
# MAGIC ## **Gold Dim_aeroporto**
# MAGIC
# MAGIC gold.dim_aeroporto - dimensao de aeroporto, servindo origem E destino.
# MAGIC
# MAGIC  A dimensao nasce do FATO, nao do cadastro. Copiar silver.aerodromos daria 496 linhas e deixaria 218 aeroportos do fato orfaos (os estrangeiros, que a ANAC nao cadastra). Uma dimensao existe para servir o fato: entao a lista de chaves vem do fato, e o cadastro ENRIQUECE por LEFT JOIN.
# MAGIC
# MAGIC  Regra de negocio que nasce aqui: a classificacao pais_aeroporto pelo prefixo ICAO. Isso nao podia estar na silver - e interpretacao, nao aritmetica.   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS voebem.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE voebem.gold.dim_aeroporto AS
# MAGIC WITH aeroportos_do_fato AS (
# MAGIC   SELECT DISTINCT icao_origem  AS icao FROM voebem.silver.vra WHERE icao_origem  IS NOT NULL AND icao_origem  <> ''
# MAGIC   UNION
# MAGIC   SELECT DISTINCT icao_destino AS icao FROM voebem.silver.vra WHERE icao_destino IS NOT NULL AND icao_destino <> ''
# MAGIC ),
# MAGIC -- defesa: se a ANAC republicar o cadastro com ICAO repetido, o join
# MAGIC -- multiplicaria linhas do fato sem dar erro nenhum. Hoje sao 496/496.
# MAGIC cadastro AS (
# MAGIC   SELECT icao, nome, municipio, uf_nome, municipio_servido, uf_servido_nome
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY icao ORDER BY nome) AS rn
# MAGIC     FROM voebem.silver.aerodromos
# MAGIC     WHERE icao IS NOT NULL AND icao <> ''
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   a.icao                                                              AS icao_aeroporto,
# MAGIC   -- fallback textual obrigatorio: coluna que a IA vai ler nao pode vir NULL
# MAGIC   COALESCE(c.nome, concat('AEROPORTO FORA DO CADASTRO ANAC (', a.icao, ')'))
# MAGIC                                                                       AS nome_aeroporto,
# MAGIC   c.municipio                                                         AS municipio_aeroporto,
# MAGIC   c.uf_nome                                                           AS uf_aeroporto,
# MAGIC   CASE WHEN a.icao RLIKE '^S[BDIJNSW]' THEN 'Brasil' ELSE 'Exterior' END
# MAGIC                                                                       AS pais_aeroporto,
# MAGIC   (c.icao IS NOT NULL)                                                AS no_cadastro_anac,
# MAGIC   current_timestamp()                                                 AS _processado_em
# MAGIC FROM aeroportos_do_fato a
# MAGIC LEFT JOIN cadastro c ON a.icao = c.icao

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Gold Fatos_voos**
# MAGIC
# MAGIC  gold.fato_voos - uma linha por etapa de voo.
# MAGIC
# MAGIC  E AQUI que nascem as regras de negocio que a silver nao podia ter:
# MAGIC
# MAGIC 1. PONTUALIDADE a 15 minutos (partida_pontual / chegada_pontual).
# MAGIC       O numero 15 e decisao de cliente. Na silver ele fecharia porta;
# MAGIC       aqui e uma linha de SQL que qualquer pessoa do negocio consegue ler.
# MAGIC 2. ESCOPO domestico / internacional, a partir do tipo de linha.
# MAGIC 3. As DECISOES SOBRE A QUARENTENA do marco-06 (documentadas abaixo).
# MAGIC
# MAGIC  Companhia e codigos de operacao entram como DIMENSAO DEGENERADA: codigo e
# MAGIC  descricao no proprio fato, porque sao poucos atributos, nao mudam no tempo
# MAGIC  e o consumidor final e um LLM - cada join a menos e um erro a menos.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### DECISOES SOBRE A QUARENTENA (213.543 registros diagnosticados no marco-06)
# MAGIC
# MAGIC a) AEROPORTO FORA DO CADASTRO DA ANAC (105.932) -> MANTIDO.
# MAGIC Nao e dado invalido, e aeroporto estrangeiro. Descartar mataria a pergunta P4 do projeto. dim_aeroporto cobre 100% do fato.
# MAGIC
# MAGIC b) VOO SEM HORARIO PREVISTO (30.800) -> MANTIDO.
# MAGIC O voo aconteceu e conta em "quantos voos". Sem horario previsto nao ha atraso a calcular: a metrica fica NULL, e NULL ja se exclui sozinho de qualquer media. Zerar seria mentir.
# MAGIC
# MAGIC c) ATRASO FORA DA FAIXA PLAUSIVEL (778 partida / 823 chegada) -> LINHA
# MAGIC MANTIDA, METRICA ANULADA. Ha atrasos de ate 44.855 min (31 dias) e antecipacoes de -43.057 (30 dias): erro de data na origem, nao operacao.
# MAGIC A linha continua contando como voo; a metrica vira NULL e a coluna atraso_fora_de_faixa registra por que.
# MAGIC
# MAGIC d) EMPRESA SEM CADASTRO (69) -> MANTIDA, com nome de fallback.
# MAGIC
# MAGIC e) DUPLICATA EXATA (41) -> REMOVIDA. Unica exclusao de linha desta camada conta-la duas vezes infla voos, cancelamentos e atraso ao mesmo tempo. Grao esperado: 1.014.705 - 41 = 1.014.664.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE voebem.gold.fato_voos AS
# MAGIC WITH vra_sem_duplicata AS (
# MAGIC   SELECT * FROM (
# MAGIC     SELECT *,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY icao_empresa, numero_voo, codigo_di, codigo_tipo_linha,
# MAGIC                      icao_origem, icao_destino, partida_prevista, partida_real,
# MAGIC                      chegada_prevista, chegada_real, situacao_voo
# MAGIC         ORDER BY _ingerido_em
# MAGIC       ) AS _rn
# MAGIC     FROM voebem.silver.vra
# MAGIC   )
# MAGIC   WHERE _rn = 1
# MAGIC ),
# MAGIC empresa AS (
# MAGIC   SELECT icao, razao_social, origem_cadastro
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (
# MAGIC       PARTITION BY icao
# MAGIC       ORDER BY CASE WHEN situacao = 'ATIVA' THEN 0 ELSE 1 END, razao_social
# MAGIC     ) AS rn
# MAGIC     FROM voebem.silver.empresas
# MAGIC     WHERE icao IS NOT NULL AND icao <> ''
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC di AS (
# MAGIC   SELECT codigo, descricao FROM voebem.silver.codigos_operacao WHERE dominio = 'codigo_di'
# MAGIC ),
# MAGIC tipo_linha AS (
# MAGIC   SELECT codigo, descricao FROM voebem.silver.codigos_operacao WHERE dominio = 'codigo_tipo_linha'
# MAGIC ),
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     v.*,
# MAGIC     -- decisao (c): metrica fora da faixa plausivel vira NULL, linha fica
# MAGIC     (v.atraso_partida_min IS NOT NULL AND (v.atraso_partida_min < -120 OR v.atraso_partida_min > 1440))
# MAGIC       OR (v.atraso_chegada_min IS NOT NULL AND (v.atraso_chegada_min < -120 OR v.atraso_chegada_min > 1440))
# MAGIC                                                                     AS atraso_fora_de_faixa
# MAGIC   FROM vra_sem_duplicata v
# MAGIC )
# MAGIC SELECT
# MAGIC   -- ===== dimensao degenerada: companhia =====
# MAGIC   b.icao_empresa,
# MAGIC   COALESCE(e.razao_social, concat('COMPANHIA NAO CADASTRADA (', b.icao_empresa, ')'))
# MAGIC                                                                     AS nome_companhia,
# MAGIC   e.origem_cadastro                                                 AS cadastro_companhia,
# MAGIC   b.numero_voo,
# MAGIC
# MAGIC   -- ===== dimensoes degeneradas: codigos de operacao =====
# MAGIC   b.codigo_di,
# MAGIC   COALESCE(d.descricao, concat('Codigo nao catalogado (', b.codigo_di, ')'))
# MAGIC                                                                     AS descricao_di,
# MAGIC   b.codigo_tipo_linha,
# MAGIC   COALESCE(t.descricao, concat('Codigo nao catalogado (', b.codigo_tipo_linha, ')'))
# MAGIC                                                                     AS descricao_tipo_linha,
# MAGIC
# MAGIC   -- ===== REGRA DE NEGOCIO: escopo do voo =====
# MAGIC   CASE
# MAGIC     WHEN b.codigo_tipo_linha IN ('N', 'C') THEN 'Domestico'
# MAGIC     WHEN b.codigo_tipo_linha IN ('I', 'G') THEN 'Internacional'
# MAGIC     ELSE 'Nao classificado'
# MAGIC   END                                                               AS escopo_voo,
# MAGIC
# MAGIC   -- ===== chaves para dim_aeroporto =====
# MAGIC   b.icao_origem,
# MAGIC   b.icao_destino,
# MAGIC   concat(b.icao_origem, ' - ', b.icao_destino)                      AS rota,
# MAGIC
# MAGIC   -- ===== tempo =====
# MAGIC   b.partida_prevista,
# MAGIC   b.partida_prevista_data,
# MAGIC   b.partida_prevista_hora,
# MAGIC   hour(b.partida_prevista)                                          AS hora_partida_prevista,
# MAGIC   CASE dayofweek(b.partida_prevista_data)
# MAGIC     WHEN 1 THEN 'domingo'  WHEN 2 THEN 'segunda' WHEN 3 THEN 'terca'
# MAGIC     WHEN 4 THEN 'quarta'   WHEN 5 THEN 'quinta'  WHEN 6 THEN 'sexta'
# MAGIC     WHEN 7 THEN 'sabado'
# MAGIC   END                                                               AS dia_semana,
# MAGIC   date_trunc('MONTH', b.partida_prevista_data)                      AS mes_referencia,
# MAGIC   b.partida_real,
# MAGIC   b.chegada_prevista,
# MAGIC   b.chegada_real,
# MAGIC
# MAGIC   -- ===== metricas (decisao (c) aplicada) =====
# MAGIC   CASE WHEN b.atraso_fora_de_faixa THEN NULL ELSE b.atraso_partida_min  END AS atraso_partida_min,
# MAGIC   CASE WHEN b.atraso_fora_de_faixa THEN NULL ELSE b.atraso_chegada_min  END AS atraso_chegada_min,
# MAGIC   CASE WHEN b.atraso_fora_de_faixa THEN NULL ELSE b.minutos_recuperados END AS minutos_recuperados,
# MAGIC   b.atraso_fora_de_faixa,
# MAGIC
# MAGIC   -- ===== REGRA DE NEGOCIO: pontualidade a 15 minutos =====
# MAGIC   CASE WHEN b.atraso_fora_de_faixa OR b.atraso_partida_min IS NULL THEN NULL
# MAGIC        ELSE b.atraso_partida_min <= 15 END                          AS partida_pontual,
# MAGIC   CASE WHEN b.atraso_fora_de_faixa OR b.atraso_chegada_min IS NULL THEN NULL
# MAGIC        ELSE b.atraso_chegada_min <= 15 END                          AS chegada_pontual,
# MAGIC
# MAGIC   -- ===== situacao =====
# MAGIC   b.situacao_voo,
# MAGIC   (b.situacao_voo = 'CANCELADO')                                    AS voo_cancelado,
# MAGIC   (b.situacao_voo = 'REALIZADO')                                    AS voo_realizado,
# MAGIC
# MAGIC   current_timestamp()                                               AS _processado_em
# MAGIC FROM base b
# MAGIC LEFT JOIN empresa    e ON b.icao_empresa      = e.icao
# MAGIC LEFT JOIN di         d ON b.codigo_di         = d.codigo
# MAGIC LEFT JOIN tipo_linha t ON b.codigo_tipo_linha = t.codigo

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Gold Obt_voos**
# MAGIC
# MAGIC gold.obt_voos - One Big Table, desenhada para um consumidor especifico: uma IA.
# MAGIC
# MAGIC  Uma linha por etapa de voo, com TUDO resolvido: nome de companhia, nome de aeroporto de origem e destino com municipio e UF, tipo de linha por extenso, escopo, pontualidade e as metricas de atraso prontas.
# MAGIC
# MAGIC  Regra de ouro desta tabela: nenhuma coluna de codigo sem a coluna de descricao correspondente ao lado. O LLM le nome, nao codigo ICAO.
# MAGIC
# MAGIC  E o unico join que ela exige de quem consome: nenhum.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE voebem.gold.obt_voos AS
# MAGIC SELECT
# MAGIC   -- ===== companhia =====
# MAGIC   f.icao_empresa,
# MAGIC   f.nome_companhia,
# MAGIC   f.numero_voo,
# MAGIC
# MAGIC   -- ===== operacao =====
# MAGIC   f.codigo_di,
# MAGIC   f.descricao_di,
# MAGIC   f.codigo_tipo_linha,
# MAGIC   f.descricao_tipo_linha,
# MAGIC   f.escopo_voo,
# MAGIC
# MAGIC   -- ===== origem =====
# MAGIC   f.icao_origem,
# MAGIC   o.nome_aeroporto        AS nome_aeroporto_origem,
# MAGIC   o.municipio_aeroporto   AS municipio_origem,
# MAGIC   o.uf_aeroporto          AS uf_origem,
# MAGIC   o.pais_aeroporto        AS pais_origem,
# MAGIC
# MAGIC   -- ===== destino =====
# MAGIC   f.icao_destino,
# MAGIC   d.nome_aeroporto        AS nome_aeroporto_destino,
# MAGIC   d.municipio_aeroporto   AS municipio_destino,
# MAGIC   d.uf_aeroporto          AS uf_destino,
# MAGIC   d.pais_aeroporto        AS pais_destino,
# MAGIC
# MAGIC   -- ===== rota, em codigo e por extenso =====
# MAGIC   f.rota                                                            AS rota_icao,
# MAGIC   concat(coalesce(o.municipio_aeroporto, f.icao_origem),  ' - ',
# MAGIC          coalesce(d.municipio_aeroporto, f.icao_destino))           AS rota_municipios,
# MAGIC
# MAGIC   -- ===== tempo =====
# MAGIC   f.partida_prevista,
# MAGIC   f.partida_prevista_data,
# MAGIC   f.partida_prevista_hora,
# MAGIC   f.hora_partida_prevista,
# MAGIC   f.dia_semana,
# MAGIC   f.mes_referencia,
# MAGIC   f.partida_real,
# MAGIC   f.chegada_prevista,
# MAGIC   f.chegada_real,
# MAGIC
# MAGIC   -- ===== metricas =====
# MAGIC   f.atraso_partida_min,
# MAGIC   f.atraso_chegada_min,
# MAGIC   f.minutos_recuperados,
# MAGIC   f.atraso_fora_de_faixa,
# MAGIC   f.partida_pontual,
# MAGIC   f.chegada_pontual,
# MAGIC
# MAGIC   -- ===== situacao =====
# MAGIC   f.situacao_voo,
# MAGIC   f.voo_realizado,
# MAGIC   f.voo_cancelado,
# MAGIC
# MAGIC   f._processado_em
# MAGIC FROM voebem.gold.fato_voos f
# MAGIC LEFT JOIN voebem.gold.dim_aeroporto o ON f.icao_origem  = o.icao_aeroporto
# MAGIC LEFT JOIN voebem.gold.dim_aeroporto d ON f.icao_destino = d.icao_aeroporto