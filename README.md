# Pipeline de Dados: Análise Integrada da Copa do Mundo FIFA 2026
**Engenharia de Dados na Nuvem com Databricks, Delta Lake e Arquitetura Medalhão**

---

## 1. Introdução

### 1.1 Objetivo e Contexto

O objetivo deste projeto MVP (Produto Mínimo Viável) é construir um pipeline de dados na nuvem (Databricks / AWS), responsável por realizar o import dos dados brutos e heterogêneos e estruturá-los por meio da **Arquitetura Medalhão (Bronze → Silver → Gold)**. Com o suporte e a governança do **Unity Catalog**, os dados são higienizados, transformados e modelados para responder a perguntas estratégicas e gerar *insights* de alto valor.

Para a construção e validação do pipeline, foi escolhido o dataset **FIFA Open Data**, contendo informações relativas à **Copa do Mundo FIFA 2026**. Esta edição marca uma expansão histórica do torneio para **48 seleções** e **104 partidas**, introduzindo complexidades inéditas de logística, governança e análise esportiva.

A solução desenvolvida aborda frentes cruciais do evento, tais como:
* **Governança e Arbitragem:** Análise de neutralidade, distribuição por confederações e rigor disciplinar das equipes de arbitragem;
* **Gestão de Elencos:** Avaliação de valor de mercado, perfil etário (maturidade) das seleções e também por posição dos jogadores, e diversidade de clubes de origem dos atletas convocados;
* **Desempenho Tático:** Estudo de padrões e métricas associadas ao rendimento das equipes na competição.

### 1.2 Perguntas

Para melhor estrutuação este item foi divido em tópicos:

#### Tópico 1: Arbitragem e Governança Disciplinar

1. **Atribuição por Confederação:** Qual é a distribuição inicial de árbitros escolhidos para a Copa? Qual é a distribuição de árbitros por confederação (UEFA, CONMEBOL, CONCACAF, etc.) ao longo das fases do torneio? Qual confederação mais apitou jogos?

2. **Neutralidade de Arbitragem:** Existem partidas em que a confederação do árbitro principal é idêntica à de uma das seleções participantes? 

3. **Rigor Disciplinar:** Quais são os 3 árbitros mais rigorosos da competição com base no histórico de faltas marcadas e cartões aplicados?

#### Tópico 2: Seleções e Staff (Squad Metrics)

4. **Maturidade e Idade:** Qual a média de idade por posição (`GK`, `DEF`, `MID`, `FWD`) e qual setor permite atletas mais veteranos?

5. **Valor de Mercado:** Quais são as 5 seleções mais valiosas em valor total de mercado e em valor médio por atleta?

6. **Diversidade de Clubes:** Qual seleção possui a maior pulverização de atletas em diferentes clubes de origem (`total_clubes_representados`)?

### 1.3 Origem, Licença e Estrutura dos Dados Brutos

Os dados foram coletados de dados abertos no Kaggle / FIFA Open Data sob licença de uso **Open Data Commons / CC BY 4.0** (permitindo uso, adaptação e redistribuição para fins educacionais e analíticos).

#### Estrutura das Tabelas Brutas (Camada Ingestion/Bronze):

* `referees_raw` (CSV): Dados cadastrais de árbitros (`referee_id`, `name`, `country`, `confederacao`, `total_matches`, `avg_cards_per_game`).

* `squads_and_players_raw` (JSON/CSV): Dados de atletas (`player_id`, `name`, `team`, `position`, `date_of_birth`, `market_value_eur`, `club`).

* `matches_detailed_raw` (CSV): Registro de partidas (`match_id`, `stage_name`, `home_team`, `away_team`, `referee_id`, `home_fouls`, `away_fouls`, `home_yellow_cards`, etc.).

* `match_prediction_features_raw` (Parquet): Tabela contendo estatísticas cruzadas de partidas e seleções.

## 2. Carga dos Dados (Import & Sincronização Databricks / GitHub)

A etapa de carga e import dos dados brutos foi desenhada para garantir reprodutibilidade, versionamento de código e governança de dados, conectando diretamente um repositório remoto do GitHub ao ambiente de execução do Databricks Workspace.

### 2.1 Estrutura do Repositório Git

O projeto foi organizado com a seguinte hierarquia de diretórios no GitHub:

```text
 │
 ├── data/
 │ └── raw/                        # Datasets brutos (.csv, .json) do FIFA Open Data
 │
 ├── notebooks/                    # Pipelines e scripts PySpark por camada
 │ ├── 01_bronze_ingestion.py      # Carga e metadata (Bronze)
 │ ├── 02_silver_cleansing.py      # Limpeza, deduplicação e tipagem (Silver)
 │ ├── 03_gold_aggregations.py     # Modelagem dimensional e agregados (Gold)
 │ └── 04_analytics_insights.py    # Consultas de negócio e plots estatísticos
 │
 ├── docs/                         # Documentação e evidências
 │
 ├── README.md                     # Documentação oficial do projeto (MVP)
```


### 2.2 Sincronização Databricks e GitHub via Databricks GitHub App 

A integração entre o Databricks Workspace e o GitHub foi realizada através do Databricks GitHub App (https://docs.databricks.com/aws/en/repos/get-access-tokens-from-git-provider#github)


### 2.3 Processo de Upload

Os arquivos do FIFA Open Data em formato .csv (referentes às listas de convocados, arbitragem e dados de partidas) foram inicialmente organizados na pasta data/raw/.

Ao realizar o git pull na interface do Databricks Repos, os arquivos de dados ficaram imediatamente acessíveis ao ecossistema PySpark do cluster pelo caminho relativo do workspace.


## 3. Modelagem e Catálogo de Dados

O projeto adota uma abordagem híbrida de modelagem no **Unity Catalog**:

1. **Camada Silver:** Modelo Relacional e padronizado em Delta Lake.

2. **Camada Gold:** Modelo Dimensional (Star Schema) e Tabelas Agregadas otimizadas para consumo analítico e BI.

A camada Silver foi modelada de forma relacional (com a tabela de partidas atuando como entidade central de eventos), garantindo a eliminação de duplicatas e a consistência das entidades. Já a camada Gold foi estruturada em Star Schema/Data Marts desnormalizados, unindo eventos e dimensões em métricas agregadas (ex: Rigor Index, Idade Média, Valor por Atleta) para otimizar diretamente as consultas de negócio.

```text
copa_do_mundo (Catalog)
 ├── bronze (Schema)
 │    ├── referees_bronze
 │    ├── match_prediction_features_bronze
 │    ├── player_stats_bronze
 │    ├── match_team_stats_bronze
 │    ├── squads_and_players_bronze
 │    └── matches_detailed_bronze
 ├── silver (Schema)
 │    ├── referees_silver
 │    ├── match_prediction_features_silver
 │    ├── player_stats_silver
 │    ├── match_team_stats_silver
 │    ├── squads_and_players_silver
 │    └── matches_detailed_silver
 └── gold (Schema)
      ├── gold_player_performance
      ├── gold_referee_analytics
      └── gold_squad_metrics
```

## 4. Pipeline de Dados ETL

O pipeline de dados foi estruturado de forma modular em notebooks PySpark encadeados no Databricks:

1. **`01_camada_bronze.py`**: Leitura dos arquivos brutos e criação das Delta Tables Bronze.

2. **`02_camada_silver.py`**: Limpeza, deduplicação, casting, padronização de datas, enriquecimento dos dados.

3. **`03_camada_gold.py`**: Criação de tabelas analíticas para os tópicos de Arbitragem e Seleções.

4. **`04_analytics_insights.py`**: Execução das consultas SQL/PySpark e geração das estatísticas e visualizações.

O detalhamento completo do tratamento realizado encontra-se nos respectivos notebooks e as evidencias na estrutura /docs com os prints relacionados.

## 5. Qualidade de Dados

Durante a fase de auditoria da camada Bronze, foram identificadas e resolvidas as seguintes inconsistências:

1. **Valores Nulos:**

   * *Problema:* Atletas sem informação de valor de mercado (`market_value_eur = NULL`).

   * *Solução:* Preenchimento por imputação com valor "0" na camada Silver para evitar distorções e erros em somatórios.

2. **Formatação:**

   * *Problema:* texto ilegível e caracteres especiais nos nomes de jogadores e árbitros devido à problema de codificação nos dados (Mojibake)

   * *Solução:* Criação de um dicionário completo de de-para dos caracteres identificados nos dados.

3. **Cálculo da idade dos jogadores:**

   * *Problema:* O uso da função dinâmica `current_date()` alteraria as médias de idade das seleções conforme a data de execução da pipeline.

   * *Solução:* Fixou-se a data oficial de início do torneio (`2026-06-11`), dividindo os dias exatos por `365.25` e aplicando `floor()` para computar idades completas.

4. **Verificação da integridade dos dados de posse de bola (%) por partida:**

   * *Problema:* Algumas estatísticas de posse de bola das seleções por partida não somam 100% por motivos de contabilização e métricas utilziadas (ex.: quando a bola está em disputa direta). 

   * *Solução:* Casos de posse de bola das seleções por partida abaixo de 100% não foram tratados. A única verificação criada foi para garantir que a soma das duas seleções não ultrapasse 100% na mesma partida.

   O detalhamento completo do tratamento realizado encontra-se nos respectivos notebooks e as evidencias na estrutura /docs com os prints relacionados.


## 6. Análise de Dados e Respostas aos Objetivos

### Tópico 1: Arbitragem e Governança Disciplinar

#### 1. Distribuição de Árbitros por Confederação

A UEFA representa a maior fatia do quadro de arbitragem (39,3%), seguida pela CONMEBOL (21,4%) e CONCACAF (14,3%). A variação dessa distribuição com base nos árbitros escolhidos ao longo das fases da Copa é apresentada graficamente.

A UEFA e a CONMEBOL mantêm o maior volume absoluto de jogos apitados na Fase de Grupos, refletindo a maior proporção de árbitros convocados inicialmente.

Nas Fases Finais (Oitavas até a Final), há uma filtragem onde árbitros de confederações cujas seleções avançam para as fases decisivas deixam de apitar (devido à neutralidade de confederação/país), elevando a presença relativa de árbitros neutros das confederações com menor número de seleções remanescentes (como CONCACAF, AFC e CAF).

#### 2. Teste de Neutralidade (Mesma Confederação)

Historicamente, a FIFA proíbe estritamente que um árbitro apite jogos de sua própria seleção nacional. No entanto, para partidas envolvendo seleções da mesma confederação (por exemplo, um árbitro da UEFA apitando um jogo entre duas seleções europeias ou entre uma europeia e uma de outra confederação), as regras permitem em cenários específicos — especialmente em confrontos diretos entre equipes da mesma confederação ou nas fases finais do torneio.

Do total de 104 partidas na Copa, 39 partidas tiveram o árbitro e pelo menos 1 das seleções pertencentes à mesma Confederação de futebol.

#### 3. Rigor Disciplinar:

Para definir e classificar os árbitros mais rigorosos de forma robusta e matematicamente justa, não foram avaliados exclusivamente o volume absoluto de faltas apitadas, nem exclusivamente o histórico de cartões. A proposta para avaliação foi a criação do Índice Composto de Rigor da Arbitragem indice_rigor que avalia a média de faltas e a média de cartões. O objetivo da criação do índice foi evitar distorção por amostras (árbitros com poucos jogos apitados), e a intensidade da punição (um árbitro que apita muitas faltas simples é menos rigoroso do que um que expulsa um atelta por uma única falta).

Os três árbitros mais rigorosos de acordo com o critério adotado foram: Yael Falcón (Argentina), Ma Ning (China) e Jalal Jayed (Marrocos).

#### Tópico 2: Seleções e Staff (Squad Metrics)

#### 4. Maturidade e Idade:

As seleções com maiores médias de idade são: Panama (30), Irã (29,6), Colombia (29,6), Cabo verde (29,2) e Qatar (28,9).

As seleções com menores médias de idade são: Costa do Marfim (25,3), Ecuador (25,6), Bosnia e Herzegovina(26), Marrocos (26,1) e Espanha (26,2).

Para responder se existe relação entre a idade e o desempenho, analisamos a correlação estatística (ou o cruzamento entre a média de idade e a fase de eliminação/posição final)

Entre as 4 primeiras seleções da Copa temos idades médias variando de 26,2 - Espanha até 28,7 - Argentina.

A posição de goleiro permite atletas mais veteranos pelos dados analisados. É provável que isso aconteça por 2 motivos: menor exigência de intensidade física em comparação com as demais posições, e a escolha de atletas mais experientes e maduros para uma posição tão importante como goleiro. É apresentado um gráfico com a distribuição de idades por posição, média, mediana e outliers.

#### 5. Valor de Mercado:

Para analisar o Valor de Mercado na camada Gold (gold_squad_metrics), consideram-se duas abordagens complementares: o Valor Total do Elenco (soma do valor de mercado de todos os atletas convocados) e o Valor Médio por Atleta (valor total dividido pelo número de atletas na convocação oficial - 26 jogadores).

TOP 5 em Valor Total de Mercado e Valor médio por atleta: Espanha, França, Inglaterra, Portugal e Argentina (ver tabela correspondente).

#### 6. Diversidade de Clubes:

Foi criado o índice de pulverização *indice_pulverizacao* (total de clubes representados em cada seleção / total de convocados). Um índice de valor "1" indica que os 26 convocados de uma seleção jogam em clubes distintos.

O resultado é apresentado na tabela final.

Analisando os dados podemos observar que:

Cabo Verde e Suécia possuem índice "1", ou seja, todos os 26 convocados de cada seleção jogam em clubes distintos.
A campeã Espanha possui índice "0,5", ou seja, os 26 jogadores convocados estão distribuídos em apenas 13 clubes. Analisando os dados da Espanha temos: Barcelona (8 convocados), Atlético de Madrid (3), Athletic Bilbao (3) e Arsenal (3).

## 7. Autoavaliação

### 7.1 Alcance dos Objetivos

O MVP atingiu o objetivo traçado no escopo inicial. Foi possível estruturar um pipeline completo no Databricks, implementar com sucesso a Arquitetura Medalhão (Bronze/Silver/Gold) e responder de forma fundamentada e visual as perguntas formuladas.

### 7.2 Experiência pessoal

Considero que o trabalho foi muito gratificante e ao mesmo desafiador. Trabalho na área de Telecomunicações e este foi meu primeiro Sprint na pós e 1º MVP. A jornada de aprendizado do Databricks, ferramentas Git, Python e SQL foi intensa mas ao mesmo tempo muito útil e proveitosa.

A oportunidade conhecer em maiores detalhes a ferramenta Databricks foi muito boa, é uma ferramenta muito valiosa para trabalhar com dados. As dificuldades que apareceram ao longo projeto foram contornadas consultadando documentação (ex.: integração Databricks <-> Git), e usando o assistente de IA que eventualmente corrigiu alguns (talvez muitos!) erros de código Python e queries SQL.

Iniciei o trabalho analisando dados da área de Telecomunicações mas devido a qualidade dos dados (ou a falta de qualidade!) acabei trocando muito tardiamente para análise dos dados da FIFA World CUP 2026. Espero que todo o esforço seja considerado na avaliação. Infelizmente devido a falta de tempo não foi possível realizar a análise mais complexa (Tópico 3) que tinha como objetivo correlacionar dados como: posse de bola, finalizações, faltas, gols, entre outras métricas para tentar identificar o que as seleções que chegaram às fases finais tinham em comum (Dominaram a posse de bola ao longo da Copa? Foram mais eficientes nas suas finalizações? Os jogadores percorreram distâncias maiores por paritda? Atingiram maiores velocidades médias/aceleração? Defendiam com a mesma velocidade/aceleração que atacavam?) 

### 7.3 Trabalhos Futuros

Finalizar a análise do **Tópico 3 - Desempenho Tático:** Estudo de padrões e métricas associadas ao rendimento das equipes na competição. 

### 7.4 Agradecimentos

Agradeço à PUC e aos professores/instrutores por toda a disponibilidade e paciência ao longo da jornada.

---
