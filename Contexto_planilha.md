# **Contexto da Planilha: GINC e GPROB**

Esta planilha do Google Sheets é utilizada para o acompanhamento e gestão de **Incidentes Graves (Major Incidents)** e **Problemas (Major Problems)**, integrando dados extraídos do ServiceNow.

## **Visão Geral das Abas**

* **Acompanhamento Principal:** MajorProblems, MajorProblems2026, MajorIncidentes2025 e MajorIncidentes2026.  
* **Integração / Extração (ServiceNow):** Major\_ServiceNow, Problem\_ServiceNow, CTASK\_MI, RCATask\_List, PTASK\_IMP, PTASK\_RCA, Change\_MI, MP\_List e Task\_SLA.  
* **Configuração e Análise:** Manual\_Info e Configuracao.

## **Mapeamento Específico de Colunas por Aba**

### **1\. Acompanhamento Principal**

#### **MajorIncidentes2026 (Range A1:Y)**

* **A**: Ticket  
* **B**: Mês  
* **C**: Abertura  
* **D**: Encerramento  
* **E**: TTR  
* **F**: Jornada  
* **G**: Titulo  
* **H**: Impacto  
* **I**: Severidade  
* **J**: Abrangência  
* **K**: Descrição Ofensor  
* **L**: Solução  
* **M**: Origem Tecnologia ?  
* **N**: Problem  
* **O**: Tecnológico  
* **P**: Tecnologia Impactada  
* **Q**: Ofensor  
* **R**: Observação  
* **S**: Consta na tabela Impacto ?  
* **T**: MSG Disp  
* **U**: Sintoma  
* **V**: Type SM  
* **W**: SM Number  
* **X**: C/ by any Project  
* **Y**: Ponto de Discussão

#### **MajorIncidentes2025 (Range A1:Y)**

* **A**: Ticket  
* **B**: Mês  
* **C**: Abertura  
* **D**: Encerramento  
* **E**: Duração Impacto  
* **F**: Jornada  
* **G**: Titulo  
* **H**: Impacto  
* **I**: Severidade  
* **J**: Abrangência  
* **K**: Descrição Ofensor  
* **L**: Solução  
* **M**: Tecnologia  
* **N**: Problem  
* **O**: Tecnológico  
* **P**: Tecnologia Impactada  
* **Q**: Ofensor  
* **R**: OBS  
* **S**: Consta na tabela Impacto ?  
* **T**: MSG Disp  
* **U**: Sintoma  
* **V**: Type SM  
* **W**: SM Number  
* **X**: C/ by any Project  
* **Y**: Ponto de Discussão

#### **MajorProblems2026 (Range A1:L)**

* **A**: Incidente  
* **B**: Problem  
* **C**: Status  
* **D**: Responsável GPROB  
* **E**: Área Responsável  
* **F**: Data Inc  
* **G**: Data PRB Opened  
* **H**: Data PRB Closed  
* **I**: Mes PRB  
* **J**: Semana  
* **K**: Aging  
* **L**: Risco Aceito

#### **MajorProblems (Range A1:L)**

* **A**: Incidente  
* **B**: RCA  
* **C**: Status  
* **D**: Responsável  
* **E**: Área Responsável  
* **F**: Data Inc  
* **G**: Data PRB Opened  
* **H**: Data PRB Closed  
* **I**: Mes PRB  
* **J**: Semana  
* **K**: Aging  
* **L**: Risco Aceito

### **2\. Integração / Extração de Dados (ServiceNow)**

#### **Major\_ServiceNow (Range A1:AB)**

* **A**: Sys ID  
* **B**: Number  
* **C**: Opened  
* **D**: Major incident state  
* **E**: Origin  
* **F**: Problem  
* **G**: Caused by Change  
* **H**: Change Request  
* **I**: Short description  
* **J**: Caller  
* **K**: Priority  
* **L**: State  
* **M**: Assignment group  
* **N**: Assigned to  
* **O**: Updated  
* **P**: Updated by  
* **Q**: Created  
* **R**: Created by  
* **S**: Service  
* **T**: Service offering  
* **U**: Category  
* **V**: Subcategory  
* **W**: Promoted  
* **X**: Resolved  
* **Y**: Resolved by  
* **Z**: Resolution code  
* **AA**: Resolution notes  
* **AB**: Closed

#### **Problem\_ServiceNow (Range A1:P)**

* **A**: Sys ID  
* **B**: Number  
* **C**: Incident  
* **D**: Opened  
* **E**: Problem statement  
* **F**: Description  
* **G**: Priority  
* **H**: State  
* **I**: Assignment group  
* **J**: Assigned to  
* **K**: Service  
* **L**: Service offering  
* **M**: Cause notes  
* **N**: Workaround  
* **O**: Fix notes  
* **P**: Resolution code

#### **PTASK\_IMP (Range A1:W)**

* **A**: Sys ID  
* **B**: Number  
* **C**: Problem  
* **D**: Incident  
* **E**: Short description  
* **F**: Description  
* **G**: Type  
* **H**: Priority  
* **I**: State  
* **J**: Close code  
* **K**: Assignment group  
* **L**: Assigned to  
* **M**: Cause Problem  
* **N**: Source process  
* **O**: Cause notes  
* **P**: Action Plan  
* **Q**: Workaround  
* **R**: Due date  
* **S**: Close notes  
* **T**: Created  
* **U**: Created by  
* **V**: Updated  
* **W**: Updated by

#### **PTASK\_RCA (Range A1:Y)**

* **A até W**: *(Mesmas colunas da aba PTASK\_IMP)*  
* **X**: RCA Tipo de Causa Raiz reclassificado pela IA  
* **Y**: RCA Processo de origem (Source Process ServiceNow) reclassificado pela IA

#### **CTASK\_MI (Range A1:P)**

* **A**: Number  
* **B**: Short description  
* **C**: Description  
* **D**: Type  
* **E**: State  
* **F**: Planned start date  
* **G**: Planned end date  
* **H**: Assignment group  
* **I**: Assigned to  
* **J**: Close code  
* **K**: Close notes  
* **L**: Created  
* **M**: Created by  
* **N**: Updated  
* **O**: Updated by  
* **P**: Closed

#### **Change\_MI (Range A1:Z)**

* **A**: Number  
* **B**: Template Type  
* **C**: Short description  
* **D**: Description  
* **E**: Type  
* **F**: Conflict status  
* **G**: State  
* **H**: Service  
* **I**: Service offering  
* **J**: Planned start date  
* **K**: Planned end date  
* **L**: Assignment group  
* **M**: Assigned to  
* **N**: Implementation plan  
* **O**: Justification  
* **P**: Risk and impact analysis  
* **Q**: Test plan  
* **R**: Backout plan  
* **S**: Reason  
* **T**: Risk  
* **U**: Category  
* **V**: Subcategory  
* **W**: The change causes any unavailability?  
* **X**: Start date of unavailability  
* **Y**: End date of unavailability  
* **Z**: Were the necessary requirements inserted/attached in the change?

#### **RCATask\_List (Range A1:K)**

* **A**: number  
* **B**: state  
* **C**: opened\_at  
* **D**: closed\_at  
* **E**: short\_description  
* **F**: priority  
* **G**: assignment\_group  
* **H**: assigned\_to  
* **I**: sys\_updated\_on  
* **J**: sys\_updated\_by  
* **K**: problem\_task\_type

#### **MP\_List (Range A1:N)**

* **A**: number  
* **B**: opened\_at  
* **C**: closed\_at  
* **D**: short\_description  
* **E**: priority  
* **F**: state  
* **G**: category  
* **H**: assignment\_group  
* **I**: assigned\_to  
* **J**: sys\_updated\_on  
* **K**: sys\_updated\_by  
* **L**: fix\_notes  
* **M**: workaround  
* **N**: close\_notes

### **3\. Configuração e Análise**

#### **Configuracao (Range A1:D)**

* **A**: Nome da Conf  
* **B**: SoftFreeze  
* **C**: HardFreeze  
* **D**: Ultima Atualização

#### **Manual\_Info (Range A1:AB)**

* **A**: Major Incident/Major  
* **B**: Major Incident/Problema  
* **C**: Major Incident/Tipo de Causa Raiz (IA)  
* **D**: Major Incident/Processo de Origem (IA)  
* **E**: Major Incident/Qualidade RCA (IA)  
* **F**: Major Incident/Qualidade Plano de Ação (IA)  
* **G**: Major Incident/Major  
* **H**: Major Incident causados por SMs Deploys/Caused by Change  
* **I**: Major Incident causados por SMs Deploys/Tipo da Mudança  
* **J**: Major Incident causados por SMs Deploys/Problema  
* **K**: Major Incident causados por SMs Deploys/Tipo de Causa Raiz (IA)  
* **L**: Major Incident causados por SMs Deploys/Processo de Origem (IA)  
* **M**: Major Incident causados por SMs Deploys/Qualidade RCA (IA)  
* **N**: Major Incident causados por SMs Deploys/Qualidade Plano de Ação (IA)  
* **O**: Major Incident causados por SMs Deploys/Ambiente PRD e NãoPRD é adequado para testes de Mudança?  
* **P**: Major Incident causados por SMs Deploys/Estratégia de testes é abrangente e adequada?  
* **Q**: Major Incident causados por SMs Deploys/Major  
* **R**: Major Incident Causados por SMs Tradicionais/Change ID  
* **S**: Major Incident Causados por SMs Tradicionais/Problema  
* **T**: Major Incident Causados por SMs Tradicionais/Qualidade Rollback  
* **U**: Major Incident Causados por SMs Tradicionais/Qualidade Plano de Testes  
* **V**: Major Incident Causados por SMs Tradicionais/Testado em não-prod ?  
* **W**: Major Incident Causados por SMs Tradicionais/Tipo de Causa Raiz (IA)  
* **X**: Major Incident Causados por SMs Tradicionais/Processo de Origem (IA)  
* **Y**: Major Incident Causados por SMs Tradicionais/Qualidade RCA (IA)  
* **Z**: Major Incident Causados por SMs Tradicionais/Qualidade Plano de Ação (IA)  
* **AA**: Major Incident Causados por SMs Tradicionais/Ambiente PRD e NãoPRD é adequado para testes de Mudança?  
* **AB**: Major Incident Causados por SMs Tradicionais/Estratégia de testes é abrangente e adequada?

## **Lógica Esperada do Apps Script**

O script interage com as abas citadas para consolidar dados importados do ServiceNow (\_ServiceNow, PTASK\_\*, CTASK\_MI), calcular prazos/SLAs (ex: coluna Aging), e atualizar/popular os painéis anuais de acompanhamento (MajorIncidentes2026, MajorProblems2026, etc.).