# Template de Projeto de Dados

Este repositório é um template para organizar projetos de dados com separação clara entre:
- armazenamento dos dados por estágio (`data/`)
- código por responsabilidade (`src/`)

## Estrutura de pastas

```text
projeto-dados-template/
├── data/
│   ├── raw/
│   ├── external/
│   ├── interim/
│   ├── processed/
│   └── final/
├── src/
│   ├── config/
│   ├── extract/
│   ├── transform/
│   ├── pipelines/
│   └── utils/
├── main.py
└── README.MD
```

## Função de cada pasta

### `data/`
- `raw/`: dados brutos, sem tratamento, exatamente como foram recebidos.
- `external/`: dados de fontes externas (APIs, parceiros, arquivos públicos).
- `interim/`: dados intermediários durante transformações e validações.
- `processed/`: dados tratados e prontos para consumo analítico interno.
- `final/`: saídas finais do projeto (datasets publicados, entregas para negócio, exportações).

### `src/`
- `config/`: configurações globais do projeto (paths, parâmetros, constantes).
  - `settings.py`: centraliza caminhos base do projeto e das áreas de dados.
- `extract/`: lógica de extração/coleta de dados.
  - `fetchers.py`: funções para buscar dados de arquivos, APIs e outras fontes.
- `transform/`: regras de transformação e limpeza.
  - `sanitize.py`: padronização, limpeza e validações.
  - `reshape.py`: reestruturação de dados (pivot, melt, joins, agregações).
- `pipelines/`: orquestração do fluxo ponta a ponta.
  - `pipeline.py`: encadeia etapas de extração, transformação e carga/salvamento.
- `utils/`: utilitários reutilizáveis por todo o projeto.
  - `file_io.py`: leitura e escrita de arquivos tabulares (csv, xlsx, parquet, json, pkl, txt).

## Organização do código

O template segue uma organização por camadas:

1. `config` define parâmetros e caminhos.
2. `extract` busca os dados de origem.
3. `transform` aplica limpeza e modelagem.
4. `pipelines` orquestra as etapas na ordem correta.
5. `utils` fornece funções compartilhadas entre módulos.
6. `main.py` funciona como ponto de entrada para executar o pipeline.

## Fluxo recomendado de dados

1. Ingestão em `data/raw/` ou `data/external/`.
2. Processamento intermediário em `data/interim/`.
3. Consolidação em `data/processed/`.
4. Publicação/entrega em `data/final/`.

## Boas práticas para evolução do template

- Manter funções pequenas e específicas por módulo.
- Evitar lógica de negócio em `main.py`; concentrar em `pipelines/` e `transform/`.
- Usar `config/settings.py` para evitar caminhos hardcoded.
- Salvar versões de saída com timestamp quando fizer sentido (já suportado em `utils/file_io.py`).
- Priorizar persistência em `.parquet` para datasets analíticos.

### Por que priorizar `.parquet`

Salvar dados em `.parquet` é uma boa prática porque:

- reduz tamanho em disco com compressão eficiente;
- acelera leitura e escrita em grandes volumes;
- é colunar, permitindo ler apenas colunas necessárias (menos I/O);
- preserva melhor os tipos de dados do que CSV;
- integra bem com ferramentas de dados (Pandas, Spark, DuckDB, BigQuery, etc.).

Em resumo: para pipelines e análise, `.parquet` tende a ser mais rápido, mais barato em armazenamento e mais confiável para manter schema.