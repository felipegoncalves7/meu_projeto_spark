# Design System

**Direção visual:** "Natura Executive Living" (~75% Natura Executive / 25% Natura Living Data)  
**Fonte de verdade da marca:** Brandbook Natura Corporativa (Março 2026)  
*Este documento cobre foundations, componentes, estados de interação e regras de visualização de dados.*

---

## 1. Sistema de Tokens

### Cadeia de Herança: Brand → Product → Semantic → Component

Nenhum código Hexadecimal é criado fora desta cadeia. Um token de marca **nunca** é utilizado diretamente em um componente — ele passa obrigatoriamente por um papel de produto e uma definição semântica antes de chegar ao componente final.

```
[Brand Token]          --brand-rosa-noite (#691B34)
      ↓
[Product Token]        --product-color-alert-strong (papel genérico de "alerta forte")
      ↓
[Semantic Token]       --semantic-status-critico (status Crítico - provisório)
      ↓
[Component Token]      --component-statuschip-critico-bg (chip de status, tabela, card)
```

> **Nota:** O par Semântico/Componente de status utiliza a Alternativa 4c das opções apresentadas (marcado como **provisório** até validação final).

---

## 2. Foundations: Brand Colors

*Extraídas do Brandbook Março 2026.*

| Nome do Tom | Hexadecimal |
| :--- | :--- |
| **Laranja Hero** | `#FE772C` |
| **Laranja Dia** | `#FF6D00` |
| **Amarelo Dia** | `#FFB338` |
| **Verde Dia** | `#00F1AE` |
| **Azul Dia** | `#00F6FF` |
| **Verde Tarde** | `#77985B` |
| **Rosa Noite** | `#691B34` |
| **Verde Noite** | `#333D20` |
| **Azul Bruma** | `#EAEFF0` |
| **Laranja Bruma** | `#FAEEE6` |
| **Branco** | `#FFFFFF` |
| **Preto** | `#000000` |

---

## 3. Foundations: Product & Semantic Colors

### Product Colors (Interface)

| Token | Papel de Interface | Mapeia para |
| :--- | :--- | :--- |
| `--product-surface-default` | Superfície principal da página | Branco |
| `--product-surface-subtle` | Superfície secundária (trilhos, fundos, hover) | Azul Bruma |
| `--product-text-primary` | Texto e números principais (substitui o preto absoluto) | Verde Noite |
| `--product-text-secondary` | Texto de apoio e metadados | Verde Noite (55% opacidade) |
| `--product-border` | Divisórias e contornos | Verde Noite (14% opacidade) |
| `--product-accent-brand` | Presença institucional (marca) | Laranja Hero |
| `--product-accent-interactive` | Preenchimentos interativos (réguas, barras) | Laranja Dia |
| `--product-focus-ring` | Contorno de foco via teclado | Azul Dia |

### Semantic Colors (Status — Alternativa Provisória 4c)

| Status | Token Semântico | Hexadecimal | Símbolo Obrigatório |
| :--- | :--- | :--- | :---: |
| **Crítico** | `--semantic-status-critico` | `#691B34` | `✕` |
| **Atenção** | `--semantic-status-atencao` | `#FFB338` | `!` |
| **Meta** | `--semantic-status-meta` | `#77985B` | `✓` |
| **Desafio** | `--semantic-status-desafio` | `#00F1AE` | `★` |

> **Regra Fixa:** A sinalização de status **nunca** deve depender apenas da cor isolada. É obrigatório o uso conjunto de **Cor + Rótulo Textual + Símbolo/Ícone**.

---

## 4. Foundations: Tipografia

**Famílias Proprietárias:** Natura Display / Natura Text / Natura Micro  
*Fallback em Protótipo:* Aptos Display / Aptos / Aptos (sem distribuição do arquivo físico de fonte).

| Nível / Estilo | Especificação | Aplicação Exemplo |
| :--- | :--- | :--- |
| **Display Grande** | 76px / Peso 300 | Números principais de KPI (`99,84%`) |
| **Display Médio** | 40px / Peso 300 | Títulos de destaque (`Jornada CB`) |
| **Text Título** | 20px / Peso 600 / Altura de linha 1.3 | Títulos de seção e modais |
| **Text Rótulo** | 14px / Peso 600 | Rótulos de itens e nomes de países |
| **Text Corrido** | 13px / Peso 400 / Altura de linha 1.5 | Descrições e Tooltips |
| **Micro Eyebrow** | 11px / Peso 700 / Caixas altas (Upper) | Cabeçalhos de seção / Eyebrows |
| **Micro Legenda** | 10px / Peso 500 | Metadados, fontes e timestamps |

> **Adaptação de Design:** O uso de caixa alta (uppercase) em *eyebrows* foi mantido para favorecer o escaneamento visual dos painéis de dados funcionais.

---

## 5. Foundations: Spacing, Grid, Bordas & Elevação

### Escala de Espaçamento Base 4px
* **Tamanhos Suportados:** 4, 8, 12, 16, 20, 24, 32, 40, 56 (px).
* **Espaçamento interno de seção:** 26–28px.
* **Blocos da arquitetura:** Contínuos (espaçamento 0), divididos apenas por uma linha hairline de 1px.
* **Grid:** 12 colunas, reinterpretando o conceito temporal do Brandbook (divisões em blocos de 25%, 33.3%, 50% e 100%).

### Bordas, Radius e Elevação

| Token | Valor | Aplicação |
| :--- | :--- | :--- |
| `--radius-sm` | 3px | Chips de status, botões, cards de fluxo |
| `--radius-track` | 3–4px | Trilhos de barra e réguas |
| `--radius-none` | 0px | Seções, cabeçalho e linhas de tabela |
| `--border-hairline` | 1px (`#333D20` a 14%) | Divisórias gerais de seções e tabelas |
| `--border-status` | 1.5px (Cor do status) | Destaque em cards com desvio |
| `--elevation` | Nenhuma sombra por padrão | Hierarquia visual definida por tipografia e bordas |
| `--elevation-overlay` | `0 2px 8px rgba(0,0,0,0.12)` | Exclusivo para Tooltips e Dropdowns |

---

## 6. Iconografia & Grafismos

* **Iconografia:** Desenhada na malha de $25 \times 25$ módulos (traço de 1 módulo, 60–90% linhas contínuas, 10–40% elementos pontilhados).
* **Grafismo de Pontos:** Restrito à seção Hero; gradiente na proporção 1:3 entre segmentos; opacidade baixa ($\le 12\%$); utiliza a cor Laranja institucional. Não deve obstruir números, rótulos ou formar figuras figurativas.

---

## 7. Componentes de Interface

1. **Header:** Altura de 44–48px; fundo branco com borda hairline. Contém título, marca d'água/ponto Laranja Hero, seletores de período, timestamp e filtro.
2. **KPI Hero:** Valor principal em Display 76px/300. Delta textual (`▼` ou `▲`) indicando variação em pontos percentuais com a cor do status correspondente.
3. **Régua Mínimo/Meta/Desafio:** Trilho de 6px com fundo neutro; preenchimento interativo em Laranja Dia; contém 3 ticks fixos e indicador do valor atual.
4. **Ranking de Países:** Linha em grid fixo (Bandeira + Nome + Barra comparativa + Valor + Gap + Chip de status). Animação de hover em Azul Bruma.
5. **Cards de Jornada e Linhas de Fluxo:** Cards com borda hairline (ou 1.5px na cor do status em caso de alerta); exibe nome, valor, metadado e status.
6. **Gráficos de Tendência:** Série temporal em barras verticais finas; destaca o período atual em Laranja Dia; sem 3D ou elementos pesados.
7. **Tooltips:** Fundo em Verde Noite, texto em branco, sombra sutil overlay. Apresenta o valor do período, valores de referência, delta e o caminho hierárquico completo.

---

## 8. Estados de Interação & Data Viz

| Estado | Comportamento da Interface |
| :--- | :--- |
| **Default** | Superfície branca neutra, texto principal em Verde Noite. |
| **Hover** | Fundo da linha/card muda para Azul Bruma (`--component-row-hover-bg`). |
| **Focus** | Anel de foco destacado com contorno de 2px em Azul Dia (`--product-focus-ring`). |
| **Loading** | Carregamento via Skeleton em cada bloco de forma independente. |
| **Error State** | Exibe a última atualização válida acompanhada do botão "Tentar novamente". |
| **Valores Ausentes**| Sinalizados explicitamente como "sem dado no período" (nunca exibir 0% ou célula em branco). |