# Contexto do Dashboard de Indicadores ITSM (IT Service Management)

Este documento foi preparado para contextualizar o **Claude Code** sobre a estrutura de indicadores de desempenho (KPIs) de Gestão de Serviços de TI existentes no sistema, bem como a especificação para incorporação de novos indicadores mapeados em benchmarking de mercado.

---

## 1. Visão Geral do Projeto e Objetivos

O objetivo deste dashboard é monitorar a saúde operacional, a estabilidade dos ambientes produtivos, a eficiência na resolução de crises (Major Incidents), a maturidade da Gestão de Problemas (Problem Management) e o impacto das Mudanças (Change Management / GMUD / CI-CD) no negócio.

---

## 2. Catálogo de Indicadores Existentes (Atuais)

### 2.1. Visão Geral & Performance de Incidentes Críticos (Major Incidents)

| Nome do KPI | Fórmula de Cálculo | Filtros Globais / Recortes | Meta / Target | Baseline Historico | Componente de Interface (UI) | Funcionalidade e Propósito |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Volume de Incidentes (Total Geral)** | `COUNT(Major Incidents)` (Sev 0 + Sev 1) | Ano de Referência, Período temporal | N/A | N/A | **Card (Resumo)** com exportação | Apresentar o volume absoluto consolidado de incidentes críticos. |
| **MTTR Médio Total** | `AVG(Mean Time to Repair)` em todos os MIs | Ano de Referência | N/A | 06:42h (2025) | **Card (Resumo)** em formato `HH:MM` | Acompanhar o tempo médio global consumido para restauração do ambiente. |
| **Incidentes e MTTR - SEV 0** | `COUNT(MIs)` + `% do Total` + `AVG(MTTR)` onde `Severidade = Sev0` | Ano de Referência, `Severidade = Sev0` | MTTR ≤ 02:00h | 04:29h (2025) | **Card Destaque (Vermelho)** | Monitoramento estrito das indisponibilidades críticas máximas contra o SLA/OLA. |
| **Incidentes e MTTR - SEV 1** | `COUNT(MIs)` + `% do Total` + `AVG(MTTR)` onde `Severidade = Sev1` | Ano de Referência, `Severidade = Sev1` | MTTR ≤ 06:00h | 09:29h (2025) | **Card Destaque (Laranja/Amarelo)** | Acompanhar a volumetria e o tempo de atendimento das crises de alto impacto. |
| **Evolução Mensal (Incidentes)** | `COUNT(MIs)` agrupado no tempo | Semanal, Mensal, Trimestral | N/A | Comparativo YoY (2026 vs 2025) | **Gráfico de Linha / Tendência** | Identificar sazonalidades e tendências de aumento ou redução na ocorrência de incidentes. |
| **Incidentes / MTTR por Tecnologia Impactada** | `COUNT(MIs)` e `AVG(MTTR)` agrupados por Sistema/Tecnologia | Ano de Referência, Drill-down/Ranking | N/A | N/A | **Gráfico de Barras Duplas (Paralelo)** | Identificar quais componentes sistêmicos geram mais volume ou demoram mais para recuperar. |
| **Incidentes / MTTR por Ofensor** | `COUNT(MIs)` e `AVG(MTTR)` agrupados por Equipe / Área / Fornecedor | Ano de Referência, Drill-down/Ranking | N/A | N/A | **Gráfico de Barras Horizontal** | Redirecionar governança para atuar nos principais causadores de instabilidades. |
| **Origem da Detecção do Incident** | `COUNT(MIs)` agrupado por `origem_deteccao` (Monitoria, Usuário, etc.) | Filtros Globais | N/A | N/A | **Gráfico de Rosca (Donut)** | Validar eficiência da observabilidade automática vs. incidentes notificados pelo cliente/usuário. |
| **Dispersão MTTD vs MTTR** | Mapeamento cartesiano: `X = MTTD`, `Y = MTTR` por incidente | Filtros Globais (Destaque em MTTD > 25h ou MTTR > 6h) | N/A | N/A | **Gráfico de Dispersão (Scatter Plot)** | Identificar outliers e anomalias operacionais (incidentes ocultos ou de longa duração). |

---

### 2.2. Gestão de Problemas (Problem Management) & Causa Raiz (RCA)

| Nome do KPI | Fórmula de Cálculo | Filtros Globais / Recortes | Meta / Target | Baseline Historico | Componente de Interface (UI) | Funcionalidade e Propósito |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Total de Problemas** | `COUNT(Tickets de Major Problem)` | Ano de Referência | N/A | N/A | **Card KPI (Laranja)** | Avaliar a carga de trabalho e o backlog total no processo de Problem Management. |
| **Problemas por Status Operacional** | `COUNT(Problemas)` segmentado por Status (`Abertos`, `Resolvidos`, `Em Implementação`, `Em RCA`) | Status do Problema | N/A | N/A | **4 Cards Indivudais** (Absoluto + %) | Monitorar a saúde do pipeline de resolução e gargalos operacionais em RCA/Implementação. |
| **Aging AVG Resolvidos (Dias)** | `AVG(Data Fim - Data Criação)` para problemas resolvidos | `Status = Resolvido` | 25d a 30d (Desafio: ≤ 24d) | 31d a 40d | **Card KPI com Thresholds** (Verde/Azul) | Medir a agilidade na conclusão de problemas estruturais e recorrentes. |
| **MTTRC (Horas)** | `AVG(Tempo em horas)` para identificar e documentar a Causa Raiz (Root Cause) | Todos os Problemas com RCA | Target: ≤ 7 dias (168h) | N/A | **Card KPI (Destaque)** | Medir a celeridade do processo de investigação das causas fundamentais das falhas. |
| **Aging AVG Backlog (Dias)** | `SUM(Dias envelhecendo nos estados RCA + Implementação) / COUNT(Backlog Pendente)` | Status de Pendência | N/A | N/A | **Card KPI** | Monitorar se pendências sem solução estão envelhecendo excessivamente no ambiente. |
| **Evolução Mensal / Semanal de Problemas** | Cruzamento de `COUNT(Resolvidos)` x `AVG(Aging)` ao longo do tempo | YTD (Mensal) e WTD (Semanal) | N/A | N/A | **Gráfico Combo (Barras + Linha)** | Analisar a capacidade de vazão do time versus o tempo médio exigido para resolução. |
| **TOP 5 Áreas: Backlog vs Resolução** | Ranking de áreas ordenado por volume de pendências vs. volume de resoluções | Top 5 Areas | N/A | N/A | **Tabelas / Listas Rankeadas** | Evidenciar equipes com retenção de backlog vs. equipes de alta performance em RCA. |
| **Causas Raiz e Processos de Origem** | `COUNT(MIs)` por `rca_causa` e `rca_processo` | Interativo / Drill-down | N/A | N/A | **Donut (Causa Raiz) + Barras (Processo)** | Análise qualitativa cruzada das causas fundamentais das paradas operacionais. |

---

### 2.3. Impacto de Mudanças (Change Management / GMUD & CI-CD Pipelines)

| Nome do KPI | Fórmula de Cálculo | Filtros Globais / Recortes | Meta / Target | Baseline Historico | Componente de Interface (UI) | Funcionalidade e Propósito |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Major Incidents Causados por SM** | `COUNT(MIs)` onde `causa_base = Mudança` | Período, Tipo de Mudança, Severidade | N/A | N/A | **Card Header (Destaque)** | Mensurar o volume total de crises decorrentes de intervenções e entregas no ambiente. |
| **SMs Únicas Causadoras** | `COUNT(DISTINCT chg_id)` associados a Major Incidents | Período, Tipo de Mudança | N/A | N/A | **Card Header** | Identificar a quantidade de mudanças únicas que geraram incidentes críticos. |
| **Aderência OLA** | `(COUNT(Dentro OLA) / TOTAL_VÁLIDOS) * 100` | Período, Exclui 'N/A' | Target OLA | N/A | **Card Header (%)** | Visão consolidada do cumprimento do acordo de nível operacional de restauração. |
| **MTTD avg Início CHG** | `AVG(Data Criacao INC - CHG Data Inicio Planejada)` em horas | Filtros Globais | ≤ 25h | N/A | **Card KPI com SLA** | Medir o tempo decorrido até a detecção do incidente após o início da janela da mudança. |
| **MTTD avg Término CHG (Deploy/CI-CD)** | `AVG(Data Criacao INC - CHG Data Termino Planejada)` focado em Deploys | Apenas Deploys CI/CD | ≤ 6h | N/A | **Card KPI com SLA** | Monitorar rigorosamente a detecção de anomalias advindas de entregas contínuas automatizadas. |
| **Aderência Sev x Prio** | `% de Sev0/Sev1` cadastrados no ServiceNow como `Prioridade 1 (Critical)` ou `Prioridade 2 (High)` | Apenas Sev0 e Sev1 | ≥ 80% | N/A | **Card KPI + Banner Informativo** | Garantir conformidade de governança (severidade técnica alinhada à prioridade do chamado). |
| **Divergências P3/P4/P5** | `COUNT(MIs)` com incoerência de severidade/prioridade (`has_gap = true`) | Filtros Globais | 0 | N/A | **Card KPI + Tabela Detalhada** | Evidenciar desalinhamentos operacionais e tickets com classificação incorreta. |
| **% Tradicionais Testadas em Não-Prod** | `% de GMUDs Tradicionais` com flag `chg_testado_nao_prod = Sim` | Apenas Mudanças Tradicionais | 100% | N/A | **Card KPI + Gráfico Rosca** | Auditar a conformidade de testes em ambientes homologados/não-produtivos antes do deploy. |
| **Qualidade de Testes e Roteiro QA** | `% de Mudanças` classificadas como `chg_qualidade_testes = Bom` | Apenas Mudanças Tradicionais | N/A | N/A | **Card KPI + Rosca** | Avaliar a maturidade dos roteiros de QA e planos de testes anexados às requisições. |
| **Avaliação Qualitativa (RCA e Planos de Ação)** | `% de registros` com `qualidade_rca = Boa` e `qualidade_acoes = Boa` | Filtros Globais | N/A | N/A | **2 Cards KPI (%)** | Medir o rigor e profundidade técnica das análises pós-incidente e planos preventivos. |
| **Distribuição por Origem (Deploy vs Tradicional)** | Matriz cruzando `origem_mudanca` x `Severidade (Sev0 / Sev1)` | Filtros Globais | N/A | N/A | **Gráfico de Barras Empilhadas** | Identificar qual esteira de entrega (automatizada vs infra tradicional) traz maior risco. |
| **Top Services & Grupos Responsáveis** | Agrupamento por Serviço ServiceNow (`service`) e `grupo_responsavel` | Top 8 a Top 10 | N/A | N/A | **Gráficos de Barras Horizontais** | Mapear os serviços de negócio e equipes com maior índice de falhas decorrentes de mudanças. |

---

### 2.4. Análise de Picos, Outliers e Impacto Operacional

| Nome do KPI | Fórmula de Cálculo | Filtros Globais / Recortes | Meta / Target | Baseline Historico | Componente de Interface (UI) | Funcionalidade e Propósito |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Total de Incidentes no Pico** | `COUNT(MIs)` na janela de análise crítica (ex: Semanas 34 a 36) | Janela de Pico | N/A | Comparativo com Média Anual (+135.2%) | **Card KPI com Delta %** | Destacar o volume absoluto durante uma crise/onda atípica comparado com a operação normal. |
| **MTTR Consolidado Bruto** | `AVG(MTTR)` de 100% dos MIs da amostra (incluindo atípicos) | Janela de Pico | N/A | N/A | **Card KPI (`HHhMMm`)** | Medir o impacto real total sofrido pelo negócio, sem exclusão de dados. |
| **MTTR Ajustado Operacional** | `AVG(MTTR)` desconsiderando outliers de longa duração (`MTTR > 24h`) | Janela de Pico, `MTTR ≤ 24h` | N/A | Redução de -62.8% vs Bruto | **Card KPI (`HHhMMm`)** | Avaliar o tempo médio de contenção do cotidiano operacional da equipe (90%+ dos casos). |
| **Volume vs MTTR por Jornada de Negócio** | `COUNT(MIs)` (Eixo Barras) cruzado com `AVG(MTTR)` (Eixo Linha) por Jornada | Recorte Temporal / Jornada | N/A | N/A | **Gráfico Combo de Eixo Duplo** | Relacionar o volume de falhas com a complexidade de recuperação por área de negócio. |
| **Análise Analítica de Outliers (> 24h)** | Listagem analítica de registros de incidentes com `MTTR > 24h` | `MTTR > 24h` | N/A | N/A | **Tabela Analítica Responsiva** | Detalhar INC, Jornada, MTTR, Causa Raiz e Ação Preventiva das piores crises do período. |

---

## 3. Especificação de Novos Indicadores Mapeados (Benchmark / Expansão)

Abaixo estão os novos indicadores identificados em referências de mercado (ITSM, SRE e DevOps DORA Metrics) propostos para implementação no site:

| Categoria | Nome do Novo KPI | Fórmula Sugerida de Cálculo | Propósito / Benefício Técnico | Componente UI Sugerido |
| :--- | :--- | :--- | :--- | :--- |
| **Disponibilidade** | **Service Availability (% Uptime)** | `((Tempo Total do Mês - SUM(MTTR Sev0 + Sev1)) / Tempo Total) * 100` | Exibir a porcentagem real de uptime dos sistemas em relação ao SLA contratado pelo negócio (ex: 99.9%). | Card KPI + Indicador de Gauge (Manômetro) |
| **Confiabilidade** | **MTBF (Mean Time Between Failures)** | `Tempo Total de Operação sem Falhas / COUNT(Major Incidents)` | Medir a frequência e a estabilidade dos sistemas ao longo do tempo (quanto maior o tempo entre falhas, mais estável a aplicação). | Card KPI (Dias/Horas) + Linha de Tendência |
| **Qualidade / QA** | **Change Success Rate (%)** | `(GMUDs Executadas sem Incidentes / Total de GMUDs Executadas) * 100` | Avaliar a eficácia global do processo de Gestão de Mudanças e testes antes de ir para Produção. | Card KPI (%) + Gráfico de Rosca |
| **Governança** | **Incident Recurrence Rate (%)** | `(MIs Reincidentes na mesma Causa/Tecnologia em 30d / Total MIs) * 100` | Identificar falhas na eficácia das soluções definitivas aplicadas pela Gestão de Problemas. | Card KPI + Tabela de Reincidências |
| **DevOps / DORA** | **Lead Time for Changes** | `Tempo Médio entre o Commit / Chamado de Mudança e o Deploy em PRD` | Medir a agilidade e a fluidez da esteira de desenvolvimento e operações. | Gráfico de Linha / Tendência |
| **Experiência / CSAT** | **NPS / CSAT Operacional de TI** | `Média das notas de avaliação do usuário pós-encerramento de chamado` | Medir a satisfação do usuário/cliente final com o atendimento e restauração de serviços. | Card KPI + Indicador de Estrelas / Score |
| **Financeiro / Risco** | **Custo Estimado de Downtime** | `SUM(MTTR em horas * Custo/Hora da Jornada Parada)` | Trazer visibilidade executiva do impacto financeiro direto provocado por paradas operacionais. | Card KPI ($ / R$) em destaque |

> **Nota para o Claude Code:** Se houver novos indicadores específicos observados no site concorrente/externo, insira-os no formato da tabela acima sob este bloco.

---

## 4. Diretrizes Técnicas de Implementação para o Claude Code

Para garantir a coerência do código e da camada de apresentação ao desenvolver/refatorar o dashboard, siga as seguintes diretrizes:

### 4.1. Tratamento e Formatação de Dados
1. **Formato de Horas e Tempos (MTTR / MTTD):**
   - No banco/backend, armazenar durações em **minutos ou segundos inteiros**.
   - Na camada de exibição (UI), formatar como `HH:MM` ou `Xh YYm` (ex: `04:29h`).
2. **Tratamento de Outliers (Bruto vs Operacional):**
   - Manter cálculo parametrizável: `MTTR_Operacional = WHERE mttr_minutes <= 1440` (24 horas).
   - Disponibilizar chave/toggle na UI para alternar entre visão bruta e operacional.
3. **Métricas de Aderência (SLA / OLA):**
   - Excluir registros marcados como `N/A` nos denominadores de cálculo de % de Aderência OLA.
   - Thresholds visuais padrão:
     - **Verde**: Dentro da meta (ex: SLA Sev0 ≤ 2h, Sev1 ≤ 6h, Aderência Sev x Prio ≥ 80%).
     - **Amarelo/Laranja**: Atenção (próximo do limite).
     - **Vermelho**: Violação / Estouro do SLA/OLA.

### 4.2. Estrutura Visual e Componentização
- **Cards Superiores (Headers):** Exibir métricas agregadas absolutas (`Volume`, `MTTR Médio`, `Aderência OLA %`).
- **Gráficos Combinados (Combo Bar + Line):** Utilizar em análises de volume vs tempo (eixo Y1 = Contagem de Incidentes, eixo Y2 = Tempo em horas).
- **Filtros Globais Interativos:**
  - `Ano / Mês / Semana (W34, W35, etc.)`
  - `Severidade (Sev0, Sev1)`
  - `Tipo de Mudança (Deploy CI/CD vs Tradicional / Infra)`
  - `Jornada de Negócio / Sistema / Tecnologia`
