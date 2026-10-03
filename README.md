<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks">
  <img src="https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white" alt="SQL">
  <img src="https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white" alt="GitHub">
</p>


# Projeto de Engenharia de Dados com IA: Aviação ANAC

Projeto desenvolvido durante a Imersão Engenharia de Dados com IA da Alura. O objetivo principal é construir um pipeline de dados robusto para ingestão, tratamento, modelagem e análise de dados do setor de aviação civil brasileira (ANAC), utilizando Databricks, Python, SQL e Inteligência Artificial.

<br>

## Tecnologias Utilizadas

* Databricks (Processamento em nuvem e notebooks)
* Python / PySpark (Manipulação e transformação de dados)
* SQL (Consultas e modelagem analítica)
* GitHub & Codespaces (Controle de versão e organização do código)

<br>

## Arquitetura do Pipeline (Medallion Architecture)
- O projeto segue a arquitetura em camadas para garantir a qualidade e governança dos dados:
- Camada Bronze: Ingestão dos dados brutos originais extraídos dos voos da ANAC, mantendo a estrutura original.
- Camada Silver: Limpeza, padronização, tipagem de colunas e remoção de inconsistências utilizando PySpark.
- Camada Gold: Modelagem dimensional estruturada para responder a perguntas de negócio e análises avançadas com IA.

<br>

## Resultados Alcançados
* Automação do Pipeline: Implementação bem-sucedida das camadas de dados (Bronze, Silver e Gold) garantindo rastreabilidade e governança.
* Padronização para Python: Migração e refatoração completa dos códigos para scripts em Python puros (`.py`), otimizando a legibilidade e o versionamento no GitHub.
* Organização Estrutural: Repositório limpo e estruturado em pasta dedicada (`arquivos/`), facilitando a manutenção e a integração contínua.
* Prontidão para IA: Dados modelados de forma dimensional na camada Gold, habilitando consultas rápidas via SQL e integração com assistentes de IA (como o Databricks Genie).

<br>

