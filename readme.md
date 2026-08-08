<p align="center">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks"/>
  <img src="https://img.shields.io/badge/Spark_SQL-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark SQL"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" alt="Pandas"/>
</p>

<h1 align="center">🔥 Por Que Seus Melhores Talentos Estão Saindo?</h1>
<h3 align="center">Painel Analítico de Retenção de Talentos & Turnover — HR Analytics com Spark SQL no Databricks</h3>

<p align="center">
  <em>Uma análise data-driven que revelou que <strong>80% dos novatos de RH com salário baixo pedem demissão</strong> — e outros insights que o departamento de Pessoas precisa saber.</em>
</p>

---

## 📋 Índice

- [Sobre o Projeto](#-sobre-o-projeto)
- [Problema de Negócio](#-problema-de-negócio)
- [Stack Técnica](#-stack-técnica)
- [Arquitetura da Solução](#-arquitetura-da-solução)
- [Como Reproduzir](#-como-reproduzir)
- [Análises e Descobertas](#-análises--descobertas)
- [Estrutura do Repositório](#-estrutura-do-repositório)
- [Dashboard](#-dashboard)
- [Principais Insights para o RH](#-principais-insights-para-o-rh)
- [Dicas Técnicas](#-dicas-técnicas)
- [Autor](#-autor)

---

## 🎯 Sobre o Projeto

Este projeto implementa um **pipeline analítico completo** no Databricks Community Edition (gratuito), desde a ingestão de dados brutos até a criação de um Dashboard executivo, utilizando exclusivamente **Spark SQL** e **Python** para responder à pergunta:

> *"Quais fatores mais impactam a evasão de funcionários e como podemos prever quem vai sair?"*

O dataset utilizado é o **IBM HR Analytics Employee Attrition**, um dataset público e fictício criado por cientistas de dados da IBM, amplamente utilizado como benchmark em projetos de People Analytics.

---

## 💼 Problema de Negócio

A área de **Recursos Humanos** enfrenta um desafio crítico: a **evasão de talentos (turnover)**. Cada funcionário que sai representa:

- 💸 **Custo de reposição** estimado entre 50% a 200% do salário anual
- 📉 **Perda de conhecimento institucional** e produtividade da equipe
- ⏱️ **Tempo de ramp-up** de 3 a 6 meses para o substituto atingir performance plena

**Objetivo:** Identificar os fatores-chave (salário, distância, satisfação, tempo de casa, departamento) que mais influenciam a decisão de um funcionário de deixar a empresa, permitindo ações preventivas e direcionadas.

---

## 🛠️ Stack Técnica

| Camada | Tecnologia | Função |
|--------|-----------|--------|
| **Plataforma** | Databricks Community Edition | Ambiente de execução gratuito na nuvem |
| **Ingestão** | Python + Pandas + Requests | Download HTTP direto para memória (bypass do DBFS) |
| **Processamento** | Apache Spark (DataFrame API) | Conversão e materialização dos dados |
| **Análise** | Spark SQL | Queries analíticas com `CASE WHEN`, agregações e cohorts |
| **Visualização** | Databricks Dashboards (nativo) | Gráficos de barras, dispersão e KPIs |
| **Versionamento** | Git + GitHub | Controle de versão e documentação |

### Por que essa stack?

- **Zero custo**: Tudo roda no tier gratuito do Databricks
- **Escalável**: Spark SQL processa desde 1.470 registros até bilhões sem alterar o código
- **Reproduzível**: Notebook auto-contido, sem dependências externas além do dataset público
- **Enterprise-ready**: Mesmas ferramentas usadas em empresas Fortune 500

---

## 🏗️ Arquitetura da Solução

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATABRICKS NOTEBOOK                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────────────┐  │
│  │  GitHub Raw   │───▶│  Pandas      │───▶│  Spark DataFrame     │  │
│  │  CSV (HTTP)   │    │  (in-memory) │    │  (processamento)     │  │
│  └──────────────┘    └──────────────┘    └──────────┬────────────┘  │
│                                                      │              │
│                                          ┌───────────▼───────────┐  │
│                                          │  Temp View: hr_data   │  │
│                                          │  (Spark SQL Engine)   │  │
│                                          └───────────┬───────────┘  │
│                                                      │              │
│                    ┌─────────────────────────────────┼────────┐     │
│                    │          SPARK SQL QUERIES       │        │     │
│                    │                                  │        │     │
│                    │  ┌─────────┐  ┌──────────────┐  │        │     │
│                    │  │ KPIs    │  │ Departamento │  │        │     │
│                    │  │ Gerais  │  │ Analysis     │  │        │     │
│                    │  └─────────┘  └──────────────┘  │        │     │
│                    │  ┌─────────┐  ┌──────────────┐  │        │     │
│                    │  │ Faixa   │  │ Perfil de    │  │        │     │
│                    │  │ Salarial│  │ Risco        │  │        │     │
│                    │  └─────────┘  └──────────────┘  │        │     │
│                    └─────────────────────────────────┘        │     │
│                                                      │        │     │
│                                          ┌───────────▼──────┐ │     │
│                                          │    DASHBOARD     │ │     │
│                                          │    (Nativo)      │ │     │
│                                          └──────────────────┘ │     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Como Reproduzir

### Pré-requisitos

- Conta gratuita no [Databricks Community Edition](https://community.cloud.databricks.com/)
- Cluster ativo (o Community Edition oferece um cluster single-node gratuito)

### Passo a Passo

**1. Crie um novo Notebook** no Databricks (linguagem padrão: `SQL`)

**2. Célula 1 — Ingestão dos Dados** (linguagem: `%python`)

```python
%python
import pandas as pd
import requests
from io import StringIO

url = "https://raw.githubusercontent.com/01Vishwa/IBM-HR-Attrition-Dashboard/main/HR-Employee-Attrition.csv"

print("Baixando dataset via HTTP...")
response = requests.get(url, timeout=30)
response.raise_for_status()

pdf_hr = pd.read_csv(StringIO(response.text))

# Converte colunas object para str (evita erro do Arrow em clusters Serverless)
for col in pdf_hr.select_dtypes(include=["object"]).columns:
    pdf_hr[col] = pdf_hr[col].astype(str)

df_hr = spark.createDataFrame(pdf_hr)
df_hr.createOrReplaceTempView("hr_data")

print(f"Sucesso! {df_hr.count()} registros carregados na view 'hr_data'.")
df_hr.printSchema()
```

> ⚠️ **Nota para clusters Serverless:** A config `spark.sql.execution.arrow.pyspark.enabled` **não está disponível**. O workaround é converter colunas `object` do Pandas para `str` antes da conversão.

**3. Células 2–8** — Cole as queries SQL do notebook (disponíveis na pasta `notebook/`)

**4. Monte o Dashboard** — Veja a seção [Dashboard](#-dashboard)

---

## 📊 Análises e Descobertas

### Visão Geral do Dataset

| Métrica | Valor |
|---------|-------|
| Total de funcionários | **1.470** |
| Funcionários que saíram | **237** |
| Taxa de atrito geral | **16,12%** |
| Colunas disponíveis | **35** |
| Valores nulos | **0** (dataset limpo) |

---

### 1. Turnover por Departamento

| Departamento | Total | Saídas | Taxa de Atrito |
|-------------|-------|--------|----------------|
| 🔴 Sales | 446 | 92 | **20,63%** |
| 🟡 Human Resources | 63 | 12 | **19,05%** |
| 🟢 Research & Development | 961 | 133 | **13,84%** |

> **Insight:** O departamento de **Vendas** tem a maior evasão, provavelmente devido à pressão por metas e alta competitividade do mercado para profissionais comerciais.

---

### 2. Turnover por Faixa Salarial

| Faixa Salarial | Total | Saídas | Taxa de Atrito |
|---------------|-------|--------|----------------|
| 🔴 Até 3k | 395 | 113 | **28,61%** |
| 🟡 3k–6k | 519 | 66 | **12,72%** |
| 🟡 6k–10k | 275 | 33 | **12,00%** |
| 🟡 10k–15k | 148 | 20 | **13,51%** |
| 🟢 Acima de 15k | 133 | 5 | **3,76%** |

> **Insight:** Quem ganha **até 3k tem 7,6x mais chance de sair** do que quem ganha acima de 15k. Salário é o fator de retenção mais forte.

---

### 3. Turnover por Tempo de Casa

| Tempo de Casa | Total | Saídas | Taxa de Atrito |
|--------------|-------|--------|----------------|
| 🔴 Até 1 ano | 215 | 75 | **34,88%** |
| 🟡 2–5 anos | 561 | 87 | **15,51%** |
| 🟢 6–10 anos | 448 | 55 | **12,28%** |
| 🟢 Mais de 10 anos | 246 | 20 | **8,13%** |

> **Insight:** **1 em cada 3 novatos sai no primeiro ano.** O processo de onboarding e os primeiros 12 meses são o período mais crítico.

---

### 4. Turnover por Satisfação no Trabalho

| Nível | Total | Saídas | Taxa de Atrito |
|-------|-------|--------|----------------|
| 🔴 Baixa (1) | 289 | 66 | **22,84%** |
| 🟡 Média (2) | 280 | 46 | **16,43%** |
| 🟡 Alta (3) | 442 | 73 | **16,52%** |
| 🟢 Muito Alta (4) | 459 | 52 | **11,33%** |

---

### 5. Turnover por Distância de Casa

| Distância | Total | Saídas | Taxa de Atrito |
|-----------|-------|--------|----------------|
| 🟢 Perto (0–5) | 632 | 87 | **13,77%** |
| 🟡 Média (6–15) | 509 | 82 | **16,11%** |
| 🔴 Longe (16+) | 329 | 68 | **20,67%** |

---

### 6. 🚨 Perfis de Maior Risco (Top 5 Combinações)

| Departamento | Cargo | Salário | Senioridade | Atrito |
|-------------|-------|---------|-------------|--------|
| Human Resources | HR | Baixo | Novato | **80,00%** |
| Sales | Sales Rep | Baixo | Novato | **56,67%** |
| R&D | Lab Technician | Baixo | Novato | **48,89%** |
| Sales | Sales Rep | Médio | Intermediário | **45,45%** |
| R&D | Lab Technician | Médio | Novato | **37,50%** |

> **🚨 Alerta crítico:** O perfil **"Novato + Salário Baixo"** é uma bomba-relógio em todos os departamentos. 4 de 5 novatos de RH com salário baixo pediram demissão.

---

## 📁 Estrutura do Repositório

```
📦 hr-analytics-turnover-databricks/
├── 📄 README.md                          # Este arquivo
├── 📁 notebook/
│   ├── 📄 01_ingestao_dados.py           # Célula 1: Ingestão via HTTP
│   ├── 📄 02_exploracao_dados.sql        # Célula 2: Exploração e validação
│   ├── 📄 03_atrito_departamento.sql     # Célula 3: Turnover por departamento
│   ├── 📄 04_atrito_faixa_salarial.sql   # Célula 4: Turnover por faixa salarial
│   ├── 📄 05_cohorts_funcionarios.sql    # Célula 5: Cohorts (distância, satisfação, tempo)
│   ├── 📄 06_scatter_idade_renda.sql     # Célula 6: Análise multifator
│   ├── 📄 07_perfil_risco.sql            # Célula 7: Ranking de perfis de risco
│   └── 📄 08_views_dashboard.sql         # Célula 8: Views para Dashboard
└── 📁 assets/
    └── 📄 arquitetura.png                # Diagrama de arquitetura (opcional)
```

---

## 📈 Dashboard

### Como montar o Dashboard nativo no Databricks

1. No notebook, clique no menu **≡** (três barras) → **New Dashboard**
2. Arraste os resultados das queries para o canvas
3. Configure os gráficos:

| Visualização | Tipo | Eixo X | Eixo Y | Cor/Grupo |
|-------------|------|--------|--------|-----------|
| KPIs Gerais | Counter | — | `taxa_atrito_pct` | — |
| Atrito por Departamento | Bar Chart | `departamento` | `taxa_atrito_pct` | `departamento` |
| Atrito por Faixa Salarial | Bar Chart | `faixa_salarial` | `taxa_atrito_pct` | — |
| Idade vs Renda | Scatter Plot | `idade` | `renda_mensal` | `atrito` |
| Perfil de Risco | Table | — | — | — |

---

## 💡 Principais Insights para o RH

### Ações Recomendadas

| Prioridade | Ação | Impacto Esperado |
|-----------|------|------------------|
| 🔴 **Urgente** | Reestruturar programa de onboarding (foco nos 12 primeiros meses) | Reduzir 34,88% de atrito em novatos |
| 🔴 **Urgente** | Revisar política salarial da faixa até 3k | Reduzir 28,61% de atrito no grupo mais vulnerável |
| 🟡 **Alta** | Plano de retenção específico para Sales Representatives | Reduzir 20,63% de atrito em Vendas |
| 🟡 **Alta** | Programa de trabalho remoto/flexível para quem mora longe | Reduzir 20,67% de atrito por distância |
| 🟢 **Média** | Pesquisa de clima + plano de ação para insatisfeitos | Reduzir 22,84% de atrito por baixa satisfação |

### Fórmula de Risco

Com base nos dados, os **3 maiores preditores de evasão** são:

```
Risco de Evasão = f(Salário Baixo, Tempo de Casa < 1 ano, Satisfação Baixa)
```

Um funcionário que combina os três fatores tem probabilidade de saída **superior a 50%**.

---

## 🧰 Dicas Técnicas

### Para quem está começando com Databricks

1. **`%python` vs `%sql`**: Use `%python` apenas na célula de ingestão. Todo o resto pode (e deve) ser feito em `%sql` puro — é mais legível e performático.

2. **Clusters Serverless**: Não suportam a config `spark.sql.execution.arrow.pyspark.enabled`. O workaround é converter colunas `object` para `str` antes do `spark.createDataFrame()`.

3. **CASE WHEN é seu melhor amigo**: Para criar cohorts (faixas) a partir de variáveis contínuas, use `CASE WHEN`. É o equivalente SQL do `pd.cut()` do Pandas.

4. **HAVING vs WHERE**: Use `WHERE` para filtrar linhas individuais e `HAVING` para filtrar grupos após um `GROUP BY`. No projeto, usamos `HAVING COUNT(*) >= 5` para excluir grupos pequenos que distorceriam as taxas.

5. **Temp Views**: São visíveis apenas dentro da sessão do notebook. Para persistir, use `CREATE TABLE` ou salve em Delta Lake.

### Para quem quer expandir o projeto

- 🤖 **Machine Learning**: Adicione um modelo de classificação (Logistic Regression, Random Forest) usando MLlib para prever atrito individual
- 📊 **Feature Engineering**: Crie features como `salario_vs_media_cargo`, `overtime_ratio`, `promocao_recente`
- 🔄 **Pipeline automatizado**: Use Databricks Workflows para agendar a atualização diária dos dados
- 📦 **Delta Lake**: Migre de Temp Views para Delta Tables para versionamento e time-travel

---

## 👤 Autor

**Alexandre Rodrigues**

---

## 📝 Licença

Este projeto utiliza o dataset [IBM HR Analytics Employee Attrition](https://www.kaggle.com/datasets/pavansubhasht/ibm-hr-analytics-attrition-dataset), um dataset público e fictício criado pela IBM para fins educacionais.

Projeto desenvolvido para fins de estudo e portfólio.

---

<p align="center">
  <strong>⭐ Se este projeto te ajudou, deixe uma estrela!</strong>
</p>
