-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## **Bronze - VRA (Voo Regular Ativo)**
-- MAGIC
-- MAGIC Lê os 12 CSVs mensais do volume `voebem.bronze.arquivos/vra/` e materializa `voebem.bronze.vra`.
-- MAGIC
-- MAGIC Regras da camada Bronze:
-- MAGIC - Nada de tipagem - tudo string, exatamente como veio do arquivo;
-- MAGIC - Nada de filtro - nenhuma linha é descartada;
-- MAGIC - Colunas de auditoria - de qual arquivo veio e quando foi ingerido;
-- MAGIC - Idempotente - rodar duas vezes e não duplica.
-- MAGIC  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC CAMINHO = "/Volumes/voebem/bronze/arquivos/vra/*.csv"
-- MAGIC TABELA = "voebem.bronze.vra"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **Leitura**
-- MAGIC
-- MAGIC Quatro opções carregam quatro problemas do arquivo:
-- MAGIC
-- MAGIC | opção | resolve |
-- MAGIC |---|---|
-- MAGIC | `sep=";"` | separador brasileiro, não vírgula |
-- MAGIC | `skipRows=1` | a 1ª linha é `Atualizado em: <data>`, não o cabeçalho - e o BOM `EF BB BF` mora nela, some junto |
-- MAGIC | `header=true` | a 2ª linha (a primeira que sobra) é o cabeçalho de verdade |
-- MAGIC | `inferSchema` **desligado** (default) | bronze não tipa: tudo chega como `string` |
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Leitura
-- MAGIC %python
-- MAGIC bruto = (
-- MAGIC     spark.read.format("csv")
-- MAGIC     .option("sep", ";")
-- MAGIC     .option("header", "true")
-- MAGIC     .option("skipRows", 1)          # descarta "Atualizado em: ..." (e o BOM junto)
-- MAGIC     .option("quote", '"')
-- MAGIC     .option("escape", '"')
-- MAGIC     .option("encoding", "UTF-8")
-- MAGIC     .option("mode", "PERMISSIVE")   # bronze nao descarta linha nenhuma
-- MAGIC     .load(CAMINHO)
-- MAGIC )
-- MAGIC
-- MAGIC print("colunas lidas do arquivo:")
-- MAGIC for c in bruto.columns:
-- MAGIC     print(f"  {c!r}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **Nomes de coluna: o Delta não aceita espaço.**
-- MAGIC
-- MAGIC ICAO Empresa Aérea` é um nome de coluna válido em CSV e inválido em Delta - espaço está na lista de caracteres proibidos (` ,;{}()\n\t=`).
-- MAGIC
-- MAGIC Então normalizamos o nome. Repare que isso não fere a regra da bronze: o que a bronze preserva é o valor e a granularidade, não a grafia do cabeçalho. Nenhuma coluna é somada, removida, filtrada ou convertida.
-- MAGIC
-- MAGIC O mapa fica explícito no código - nada de `regexp_replace` mágico, para que a correspondência com o arquivo original seja auditável.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC RENOMEAR = {
-- MAGIC     "ICAO Empresa Aérea": "icao_empresa",
-- MAGIC     "Número Voo": "numero_voo",
-- MAGIC     "Código Autorização (DI)": "codigo_di",
-- MAGIC     "Código Tipo Linha": "codigo_tipo_linha",
-- MAGIC     "ICAO Aeródromo Origem": "icao_origem",
-- MAGIC     "ICAO Aeródromo Destino": "icao_destino",
-- MAGIC     "Partida Prevista": "partida_prevista",
-- MAGIC     "Partida Real": "partida_real",
-- MAGIC     "Chegada Prevista": "chegada_prevista",
-- MAGIC     "Chegada Real": "chegada_real",
-- MAGIC     "Situação Voo": "situacao_voo",
-- MAGIC     "Código Justificativa": "codigo_justificativa",
-- MAGIC }
-- MAGIC
-- MAGIC faltando = [c for c in RENOMEAR if c not in bruto.columns]
-- MAGIC assert not faltando, f"Coluna esperada nao encontrada no CSV: {faltando}"
-- MAGIC
-- MAGIC renomeado = bruto.select(
-- MAGIC     *[F.col(f"`{origem}`").cast("string").alias(novo) for origem, novo in RENOMEAR.items()]
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **Auditoria**
-- MAGIC
-- MAGIC Duas colunas que o arquivo não tem e a tabela precisa ter: `_arquivo_origem` (de qual CSV a linha veio - `_metadata` é uma coluna oculta que o Spark expõe em qualquer leitura de arquivo) e `_ingerido_em`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC bronze = renomeado.withColumn(
-- MAGIC     "_arquivo_origem", F.col("_metadata.file_name")
-- MAGIC ).withColumn(
-- MAGIC     "_ingerido_em", F.current_timestamp()
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **Escrita idempotente**
-- MAGIC
-- MAGIC Estratégia: full refresh determinístico - `mode("overwrite")` sobre o conjunto inteiro de arquivos.
-- MAGIC
-- MAGIC Por que essa e não um `append` com deduplicação:
-- MAGIC
-- MAGIC     1-  A fonte é imutável e completa: o volume tem os 12 arquivos do mês fechado, e a ANAC republica o mês inteiro quando corrige algo. A entrada define o estado final - logo o destino pode ser derivado inteiro dela.
-- MAGIC     2 - `append` exigiria uma chave de negócio para deduplicar. O VRA não tem chave natural única (o mesmo voo pode repetir legitimamente na mesma data - veja o código DI "Etapa de Voo Duplicada"). Deduplicar no bronze seria decidir regra de negócio na camada errada.
-- MAGIC     3 - `overwrite` no Delta é atômico: ou a versão nova aparece inteira, ou a antiga continua valendo. Ninguém lê tabela pela metade.
-- MAGIC     4 - O histórico não se perde: cada `overwrite` gera uma versão nova no log do Delta, e a anterior continua acessível por time travel (marco-04).
-- MAGIC
-- MAGIC O que muda entre duas execuções: só `_ingerido_em`. O conjunto de linhas é idêntico - é isso que a validação prova.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     bronze.write.format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .saveAsTable(TABELA)
-- MAGIC )
-- MAGIC
-- MAGIC print(f"{TABELA}: {spark.table(TABELA).count():,} linhas")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC     COMMENT ON TABLE {TABELA} IS
-- MAGIC     'Bronze - VRA (Voo Regular Ativo) da ANAC, 12 meses (ago/2025 a jul/2026).
-- MAGIC      Dado bruto: todas as colunas string, nenhuma linha descartada.
-- MAGIC      Carga full refresh idempotente a partir de /Volumes/voebem/bronze/arquivos/vra/.'
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     spark.sql(f"""
-- MAGIC         SELECT _arquivo_origem, COUNT(*) AS linhas, MAX(_ingerido_em) AS ingerido_em
-- MAGIC         FROM {TABELA}
-- MAGIC         GROUP BY _arquivo_origem
-- MAGIC         ORDER BY _arquivo_origem
-- MAGIC     """)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC