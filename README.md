<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks">
  <img src="https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white" alt="SQL">
  <img src="https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white" alt="GitHub">
</p>

<br>

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

## Visão Geral do Pipeline no Databricks
<img width="1920" height="1060" alt="Captura 01" src="https://github.com/user-attachments/assets/fa969668-9571-46ea-a110-5e2d66cb3300" />

<br>

## Linhagem e Fluxo Detalhado dos Dados
<img width="3252" height="1788" alt="Captura 02" src="https://github.com/user-attachments/assets/af71920e-4770-4f5e-8af7-2a1316a92bc5" />

<br>

## Resultados Alcançados
* Automação do Pipeline: Implementação bem-sucedida das camadas de dados (Bronze, Silver e Gold) garantindo rastreabilidade e governança.
* Padronização para Python: Migração e refatoração completa dos códigos para scripts em Python puros (`.py`), otimizando a legibilidade e o versionamento no GitHub.
* Organização Estrutural: Repositório limpo e estruturado em pasta dedicada (`arquivos/`), facilitando a manutenção e a integração contínua.
* Prontidão para IA: Dados modelados de forma dimensional na camada Gold, habilitando consultas rápidas via SQL e integração com assistentes de IA (como o Databricks Genie).

<br>

## Análises com Inteligência Artificial (Databricks Genie)
Através da configuração de **Examples** (exemplos curados), treinamos o agente para compreender melhor o contexto dos dados e entregar respostas precisas sobre horários críticos, padrões de atraso e desempenho das companhias aéreas.

Utilizando o Databricks Genie, foi possível realizar perguntas em linguagem natural sobre os dados da ANAC para extrair insights valiosos sobre horários críticos, padrões de atraso de partidas ao longo do dia e desempenho das companhias aéreas.

<br>

<img width="2392" height="1330" alt="Captura 03" src="https://github.com/user-attachments/assets/b208816d-575b-4f62-a986-31a4d0920ba8" />


