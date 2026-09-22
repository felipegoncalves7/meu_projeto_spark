// ARQUIVO: Código.gs

// Constantes de Configuração
var SPREADSHEET_ID = '1WUCKPAUHWidLFc0PpVgAzehQnEfSSZLFx1qdGO_Y24Y';
const SHEET_CONFIG = "Configuracao";

// Mapeamento das Colunas (Base 0)
const COL_DURACAO = 4;     // E
const COL_SEVERIDADE = 8;  // I
const COL_TECNOLOGIA = 12; // M (Filtro base 'Sim')
const COL_TECH_IMPACTADA = 15; // P
const COL_OFENSOR = 16;    // Q
const COL_MES = 1;         // B
const COL_ABERTURA = 2;    // C
const COL_JORNADA = 5;     // F
const COL_ABRANGENCIA = 9; // J (Países Impactados)
const COL_PROBLEMA = 13;   // N
const COL_TYPE_SM = 21;    // V
const COL_SM_NUMBER = 22;  // W

// Metas de OLA (minutos) para Aderência - usadas enquanto a aba Task_SLA não é alimentada
const OLA_TARGET_SEV0_MIN = 120; // 2h
const OLA_TARGET_SEV1_MIN = 360; // 6h

// Mapeamento da aba Change_MI (Range A1:Z)
const CHG_COL_NUMBER = 0;            // A
const CHG_COL_SERVICE = 7;           // H
const CHG_COL_SERVICE_OFFERING = 8;  // I
const CHG_COL_PLANNED_START = 9;     // J
const CHG_COL_PLANNED_END = 10;      // K
const CHG_COL_ASSIGNMENT_GROUP = 11; // L

/**
 * Monta um mapa Number -> {plannedStart, plannedEnd, service, serviceOffering, assignmentGroup}
 * a partir da aba Change_MI, usado para calcular o MTTD (tempo entre início/término planejado
 * da Mudança e a abertura do incidente) e o ranking de Grupos/Serviços responsáveis.
 */
function buildChangeMap(ss) {
  const map = {};
  const sheet = ss.getSheetByName('Change_MI');
  if (!sheet) return map;

  const values = sheet.getDataRange().getValues();
  for (let i = 1; i < values.length; i++) {
    const row = values[i];
    const number = String(row[CHG_COL_NUMBER] || '').trim();
    if (!number) continue;
    map[number] = {
      plannedStart: row[CHG_COL_PLANNED_START] instanceof Date ? row[CHG_COL_PLANNED_START] : null,
      plannedEnd: row[CHG_COL_PLANNED_END] instanceof Date ? row[CHG_COL_PLANNED_END] : null,
      service: String(row[CHG_COL_SERVICE] || '').trim() || 'N/A',
      serviceOffering: String(row[CHG_COL_SERVICE_OFFERING] || '').trim() || 'N/A',
      assignmentGroup: String(row[CHG_COL_ASSIGNMENT_GROUP] || '').trim() || 'N/A'
    };
  }
  return map;
}

// Mapeamento da aba Major_ServiceNow (Range A1:AB)
const MSN_COL_NUMBER = 1;    // B
const MSN_COL_PRIORITY = 10; // K

/**
 * Monta um mapa Number -> Priority a partir da aba Major_ServiceNow, usado para calcular a
 * Aderência Sev x Prioridade (Severidade técnica do incidente vs Priority cadastrada no ServiceNow).
 */
function buildPriorityMap(ss) {
  const map = {};
  const sheet = ss.getSheetByName('Major_ServiceNow');
  if (!sheet) return map;

  const values = sheet.getDataRange().getValues();
  for (let i = 1; i < values.length; i++) {
    const row = values[i];
    const number = String(row[MSN_COL_NUMBER] || '').trim();
    if (!number) continue;
    map[number] = String(row[MSN_COL_PRIORITY] || '').trim();
  }
  return map;
}

const MSN_COL_CALLER = 9; // J

/**
 * Monta um mapa Number -> Caller a partir da aba Major_ServiceNow, usado para determinar a
 * Origem da Detecção do Incidente (End-users / Monitoração / Experiências).
 */
function buildCallerMap(ss) {
  const map = {};
  const sheet = ss.getSheetByName('Major_ServiceNow');
  if (!sheet) return map;

  const values = sheet.getDataRange().getValues();
  for (let i = 1; i < values.length; i++) {
    const row = values[i];
    const number = String(row[MSN_COL_NUMBER] || '').trim();
    if (!number) continue;
    map[number] = String(row[MSN_COL_CALLER] || '').trim();
  }
  return map;
}

/**
 * Classifica o Caller (ServiceNow) em uma das 3 origens de detecção do Incidente.
 * Monitoração: caller contém "Integração"/"Integration". Experiências: contém "Experiência".
 * Demais valores (geralmente nomes de pessoas) são considerados End-users.
 */
function classifyCaller(caller) {
  const c = String(caller || '').trim();
  if (!c) return null;
  const lower = c.toLowerCase();
  if (lower.indexOf('integra') !== -1) return 'Monitoração';
  if (lower.indexOf('experi') !== -1) return 'Experiências';
  return 'End-users';
}

// Mapeamento da aba Manual_Info (Range A1:AB) - 3 blocos lado a lado (Base 0)
// Bloco Geral (demais Incidentes, não causados por Mudança Deploy/Tradicional)
const MI_GERAL_ID = 0;                    // A
const MI_GERAL_TIPO_CAUSA_RAIZ = 2;       // C
const MI_GERAL_PROCESSO_ORIGEM = 3;       // D
const MI_GERAL_QUALIDADE_RCA = 4;         // E
const MI_GERAL_QUALIDADE_PLANO_ACAO = 5;  // F
// Bloco Deploys
const MI_DEPLOY_ID = 6;                       // G
const MI_DEPLOY_TIPO_CAUSA_RAIZ = 10;         // K
const MI_DEPLOY_PROCESSO_ORIGEM = 11;         // L
const MI_DEPLOY_QUALIDADE_RCA = 12;           // M
const MI_DEPLOY_QUALIDADE_PLANO_ACAO = 13;    // N
const MI_DEPLOY_AMBIENTE_ADEQUADO = 14;       // O
const MI_DEPLOY_ESTRATEGIA_TESTES = 15;       // P
// Bloco Tradicionais
const MI_TRAD_ID = 16;                     // Q
const MI_TRAD_QUALIDADE_ROLLBACK = 19;     // T
const MI_TRAD_QUALIDADE_PLANO_TESTES = 20; // U
const MI_TRAD_TESTADO_NAO_PROD = 21;       // V
const MI_TRAD_TIPO_CAUSA_RAIZ = 22;        // W
const MI_TRAD_PROCESSO_ORIGEM = 23;        // X
const MI_TRAD_QUALIDADE_RCA = 24;          // Y
const MI_TRAD_QUALIDADE_PLANO_ACAO = 25;   // Z
const MI_TRAD_AMBIENTE_ADEQUADO = 26;      // AA
const MI_TRAD_ESTRATEGIA_TESTES = 27;      // AB
// Colunas de Problema (RCA), usadas para deduplicar Causa Raiz/Processo de Origem/Qualidade
// quando mais de um Incidente está associado ao mesmo Problema
const MI_GERAL_PROBLEMA = 1;   // B
const MI_DEPLOY_PROBLEMA = 9;  // J
const MI_TRAD_PROBLEMA = 18;   // S

/**
 * Calcula os indicadores de Qualidade de RCA, Plano de Ação e Governança de Testes
 * a partir da aba Manual_Info (dados preenchidos manualmente pela equipe).
 * Células vazias significam que o Problema segue em aberto ou a análise não foi concluída,
 * e são excluídas do denominador de cada percentual (mesma lógica de Aderência OLA).
 */
function getQualidadeMudancaData() {
  try {
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const sheet = ss.getSheetByName('Manual_Info');
    if (!sheet) return { error: "Aba 'Manual_Info' não encontrada." };

    const values = sheet.getDataRange().getValues();
    // Linhas 1 e 2 são títulos/subtítulos dos 3 blocos; os dados começam de fato na linha 3.
    const rows = values.slice(2);

    const isFilled = (v) => v !== null && v !== undefined && String(v).trim() !== '';
    const isBoa = (v) => isFilled(v) && String(v).trim().toLowerCase().startsWith('boa');
    const isSim = (v) => isFilled(v) && String(v).trim().toLowerCase().startsWith('sim');

    /**
     * Processa um dos 3 blocos (Geral/Deploy/Tradicional) da aba Manual_Info.
     * Qualidade do RCA, Qualidade do Plano de Ação, Causa Raiz e Processo de Origem são análises
     * feitas uma vez por Problema (não por Incidente) — se 2+ Incidentes apontam para o mesmo
     * Problema, contam como 1 só nesses indicadores, mas todos os Incidentes entram na lista de
     * IDs para o drill-down. As demais perguntas (ligadas à Mudança/Change específica que gerou
     * cada Incidente) continuam contadas por Incidente/linha.
     */
    const processUniverso = (cols) => {
      let total = 0;
      const perRow = {
        ambienteFilled: 0, ambienteAdequado: 0,
        estrategiaFilled: 0, estrategiaAdequada: 0,
        rollbackFilled: 0, rollbackBoa: 0,
        planoTestesFilled: 0, planoTestesBoa: 0,
        naoProdFilled: 0, naoProdSim: 0
      };
      const problemaGroups = {}; // chave: Problema (ou ID como fallback) -> dados agregados

      rows.forEach(row => {
        if (!isFilled(row[cols.id])) return;
        const id = String(row[cols.id]).trim();
        total++;

        if (cols.ambienteAdequado !== undefined && isFilled(row[cols.ambienteAdequado])) {
          perRow.ambienteFilled++; if (isSim(row[cols.ambienteAdequado])) perRow.ambienteAdequado++;
        }
        if (cols.estrategiaTestes !== undefined && isFilled(row[cols.estrategiaTestes])) {
          perRow.estrategiaFilled++; if (isSim(row[cols.estrategiaTestes])) perRow.estrategiaAdequada++;
        }
        if (cols.rollback !== undefined && isFilled(row[cols.rollback])) {
          perRow.rollbackFilled++; if (isBoa(row[cols.rollback])) perRow.rollbackBoa++;
        }
        if (cols.planoTestes !== undefined && isFilled(row[cols.planoTestes])) {
          perRow.planoTestesFilled++; if (isBoa(row[cols.planoTestes])) perRow.planoTestesBoa++;
        }
        if (cols.naoProd !== undefined && isFilled(row[cols.naoProd])) {
          perRow.naoProdFilled++; if (isSim(row[cols.naoProd])) perRow.naoProdSim++;
        }

        const problemaRaw = row[cols.problema];
        const groupKey = isFilled(problemaRaw) ? ('P:' + String(problemaRaw).trim()) : ('I:' + id);
        if (!problemaGroups[groupKey]) {
          problemaGroups[groupKey] = { ids: [], causaRaiz: null, processoOrigem: null, qualidadeRca: null, qualidadePlanoAcao: null };
        }
        const g = problemaGroups[groupKey];
        g.ids.push(id);
        if (!g.causaRaiz && isFilled(row[cols.causaRaiz])) g.causaRaiz = String(row[cols.causaRaiz]).trim();
        if (!g.processoOrigem && isFilled(row[cols.processoOrigem])) g.processoOrigem = String(row[cols.processoOrigem]).trim();
        if (g.qualidadeRca === null && isFilled(row[cols.qualidadeRca])) g.qualidadeRca = row[cols.qualidadeRca];
        if (g.qualidadePlanoAcao === null && isFilled(row[cols.qualidadePlanoAcao])) g.qualidadePlanoAcao = row[cols.qualidadePlanoAcao];
      });

      let rcaFilled = 0, rcaBoa = 0, planoFilled = 0, planoBoa = 0;
      const causaRaizCounts = {};
      const processoOrigemCounts = {};
      const addCount = (map, label, ids) => {
        if (!map[label]) map[label] = { count: 0, ids: [] };
        map[label].count++;
        map[label].ids.push(...ids);
      };

      Object.keys(problemaGroups).forEach(key => {
        const g = problemaGroups[key];
        if (g.qualidadeRca !== null) { rcaFilled++; if (isBoa(g.qualidadeRca)) rcaBoa++; }
        if (g.qualidadePlanoAcao !== null) { planoFilled++; if (isBoa(g.qualidadePlanoAcao)) planoBoa++; }
        if (g.causaRaiz) addCount(causaRaizCounts, g.causaRaiz, g.ids);
        if (g.processoOrigem) addCount(processoOrigemCounts, g.processoOrigem, g.ids);
      });

      return {
        total, rcaFilled, rcaBoa, planoFilled, planoBoa,
        ambienteFilled: perRow.ambienteFilled, ambienteAdequado: perRow.ambienteAdequado,
        estrategiaFilled: perRow.estrategiaFilled, estrategiaAdequada: perRow.estrategiaAdequada,
        rollbackFilled: perRow.rollbackFilled, rollbackBoa: perRow.rollbackBoa,
        planoTestesFilled: perRow.planoTestesFilled, planoTestesBoa: perRow.planoTestesBoa,
        naoProdFilled: perRow.naoProdFilled, naoProdSim: perRow.naoProdSim,
        causaRaizCounts, processoOrigemCounts,
        // Flags indicando quais perguntas de Change se aplicam a este Universo
        hasAmbiente: cols.ambienteAdequado !== undefined,
        hasEstrategia: cols.estrategiaTestes !== undefined,
        hasRollback: cols.rollback !== undefined,
        hasPlanoTestes: cols.planoTestes !== undefined,
        hasNaoProd: cols.naoProd !== undefined
      };
    };

    const geral = processUniverso({
      id: MI_GERAL_ID, problema: MI_GERAL_PROBLEMA,
      causaRaiz: MI_GERAL_TIPO_CAUSA_RAIZ, processoOrigem: MI_GERAL_PROCESSO_ORIGEM,
      qualidadeRca: MI_GERAL_QUALIDADE_RCA, qualidadePlanoAcao: MI_GERAL_QUALIDADE_PLANO_ACAO
    });
    const deploy = processUniverso({
      id: MI_DEPLOY_ID, problema: MI_DEPLOY_PROBLEMA,
      causaRaiz: MI_DEPLOY_TIPO_CAUSA_RAIZ, processoOrigem: MI_DEPLOY_PROCESSO_ORIGEM,
      qualidadeRca: MI_DEPLOY_QUALIDADE_RCA, qualidadePlanoAcao: MI_DEPLOY_QUALIDADE_PLANO_ACAO,
      ambienteAdequado: MI_DEPLOY_AMBIENTE_ADEQUADO, estrategiaTestes: MI_DEPLOY_ESTRATEGIA_TESTES
    });
    const tradicional = processUniverso({
      id: MI_TRAD_ID, problema: MI_TRAD_PROBLEMA,
      causaRaiz: MI_TRAD_TIPO_CAUSA_RAIZ, processoOrigem: MI_TRAD_PROCESSO_ORIGEM,
      qualidadeRca: MI_TRAD_QUALIDADE_RCA, qualidadePlanoAcao: MI_TRAD_QUALIDADE_PLANO_ACAO,
      ambienteAdequado: MI_TRAD_AMBIENTE_ADEQUADO, estrategiaTestes: MI_TRAD_ESTRATEGIA_TESTES,
      rollback: MI_TRAD_QUALIDADE_ROLLBACK, planoTestes: MI_TRAD_QUALIDADE_PLANO_TESTES, naoProd: MI_TRAD_TESTADO_NAO_PROD
    });

    const toSortedList = (map) => Object.keys(map)
      .map(label => ({ label: label, count: map[label].count, ids: map[label].ids }))
      .sort((a, b) => b.count - a.count);

    const pct = (n, d) => d > 0 ? Math.round((n / d) * 1000) / 10 : null;

    const formatBlock = (b) => {
      const out = { total: b.total };
      out.qualidadeRcaPct = pct(b.rcaBoa, b.rcaFilled); out.qualidadeRcaBase = b.rcaFilled;
      out.qualidadePlanoAcaoPct = pct(b.planoBoa, b.planoFilled); out.qualidadePlanoAcaoBase = b.planoFilled;
      if (b.hasAmbiente) { out.ambienteAdequadoPct = pct(b.ambienteAdequado, b.ambienteFilled); out.ambienteAdequadoBase = b.ambienteFilled; }
      if (b.hasEstrategia) { out.estrategiaTestesPct = pct(b.estrategiaAdequada, b.estrategiaFilled); out.estrategiaTestesBase = b.estrategiaFilled; }
      if (b.hasRollback) { out.qualidadeRollbackPct = pct(b.rollbackBoa, b.rollbackFilled); out.qualidadeRollbackBase = b.rollbackFilled; }
      if (b.hasPlanoTestes) { out.qualidadePlanoTestesPct = pct(b.planoTestesBoa, b.planoTestesFilled); out.qualidadePlanoTestesBase = b.planoTestesFilled; }
      if (b.hasNaoProd) { out.testadoNaoProdPct = pct(b.naoProdSim, b.naoProdFilled); out.testadoNaoProdBase = b.naoProdFilled; }
      out.causaRaiz = toSortedList(b.causaRaizCounts);
      out.processoOrigem = toSortedList(b.processoOrigemCounts);
      return out;
    };

    return {
      geral: formatBlock(geral),
      deploy: formatBlock(deploy),
      tradicional: formatBlock(tradicional)
    };
  } catch (e) {
    return { error: e.toString() };
  }
}

/**
 * Busca os detalhes de um conjunto de Incidentes (por ticket) em todas as abas MajorIncidentes{ano}
 * disponíveis. Usado no drill-down: ao clicar numa Causa Raiz ou Processo de Origem recorrente,
 * mostra o período e os detalhes dos incidentes que compartilham aquele valor.
 */
function getIncidentsByIds(ids) {
  try {
    if (!ids || !ids.length) return [];
    const idSet = {};
    ids.forEach(id => { idSet[String(id).trim()] = true; });

    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const sheets = ss.getSheets();
    const regex = /^MajorIncidentes(\d{4})$/i;
    const results = [];

    const pad = n => n.toString().padStart(2, '0');
    const formatDate = (val) => {
      if (val instanceof Date) {
        return `${pad(val.getDate())}/${pad(val.getMonth() + 1)}/${val.getFullYear()} ${pad(val.getHours())}:${pad(val.getMinutes())}`;
      }
      return val ? String(val) : "N/A";
    };

    sheets.forEach(sheet => {
      const match = sheet.getName().match(regex);
      if (!match) return;

      const values = sheet.getDataRange().getValues();
      const rows = values.slice(1);
      if (rows.length === 0) return;
      const durationDisplayValues = sheet.getRange(2, COL_DURACAO + 1, rows.length, 1).getDisplayValues();

      rows.forEach((row, i) => {
        const ticketId = String(row[0] || '').trim();
        if (!ticketId || !idSet[ticketId]) return;

        const openDate = row[COL_ABERTURA] instanceof Date ? row[COL_ABERTURA] : null;
        const durRaw = durationDisplayValues[i] ? durationDisplayValues[i][0] : "00:00";

        results.push({
          id: ticketId,
          severidade: String(row[COL_SEVERIDADE] || '').trim(),
          abrangencia: String(row[9] || '').trim() || "N/A",
          descImpacto: String(row[7] || '').trim() || "N/A",
          offender: String(row[COL_OFENSOR] || '').trim() || "N/A",
          descOfensor: String(row[10] || '').trim() || "N/A",
          solucao: String(row[11] || '').trim() || "N/A",
          dataDefinicao: formatDate(row[COL_ABERTURA]),
          dataAberturaTimestamp: openDate ? openDate.getTime() : 0,
          dataEncerramento: formatDate(row[3]),
          ttr: durRaw,
          jornada: String(row[5] || '').trim() || "N/A",
          ano: match[1]
        });
      });
    });

    results.sort((a, b) => b.dataAberturaTimestamp - a.dataAberturaTimestamp);
    return results;
  } catch (e) {
    return { error: e.toString() };
  }
}

/**
 * Ponto de entrada
 */
function doGet() {
  return HtmlService.createTemplateFromFile('Index')
      .evaluate()
      .setSandboxMode(HtmlService.SandboxMode.IFRAME)
      .setTitle('Gestão de Incidentes e Problemas ITSM');
}

function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

/**
 * Helper de Duração
 */
function parseDurationString(durationString) {
  if (!durationString || typeof durationString !== 'string') return 0;
  const parts = durationString.split(':').map(p => parseInt(p.trim(), 10) || 0);
  let totalMinutes = 0;
 
  if (parts.length === 4) {
      // Formato DD:HH:MM:SS (Dia, Hora, Minuto, Segundo)
      totalMinutes = (parts[0] * 1440) + (parts[1] * 60) + parts[2];
  } else if (parts.length === 3) {
      // Formato HH:MM:SS
      totalMinutes = (parts[0] * 60) + parts[1];
  } else if (parts.length === 2) {
      // Formato HH:MM
      totalMinutes = (parts[0] * 60) + parts[1];
  } else if (parts.length === 1) {
      // Apenas minutos
      totalMinutes = parts[0];
  }
  return totalMinutes;
}

// --- NEW FUNCTION: getMudancaAnalytics ---
function getMudancaAnalytics(year, periodKey, startDate, endDate) {
    try {
        const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
        const sheetDados26 = ss.getSheetByName(`MajorIncidentes${year}`);
        if (!sheetDados26) return { error: `Dados para o ano ${year} não encontrados.` };
        
        // 1. Process Current Year (2026)
        const metrics26 = processMudancaSheet(sheetDados26, periodKey, startDate, endDate, false);

        // 2. Process Previous Year (2025) if applicable
        let metrics25 = null;
        if (year == "2026") {
            const sheetDados25 = ss.getSheetByName(`MajorIncidentes2025`);
            if (sheetDados25) {
                // Determine 2025 date range (Mirroring logic like Data Story)
                const today = new Date();
                const mirrorToday = new Date(today);
                mirrorToday.setFullYear(2025);

                let start25 = startDate ? new Date(startDate) : null;
                let end25 = endDate ? new Date(endDate) : null;

                if (periodKey === 'All') {
                    start25 = new Date(2025, 0, 1, 0, 0, 0);
                    end25 = mirrorToday;
                } else {
                    if (start25) start25.setFullYear(2025);
                    if (end25) {
                        end25.setFullYear(2025);
                        if (end25 > mirrorToday) end25 = mirrorToday;
                    } else if (!end25 && periodKey) {
                        end25 = mirrorToday;
                    }
                }

                // Process 2025 using the EXACT mirrored period
                const exact25 = processMudancaSheet(sheetDados25, periodKey, start25 ? start25.getTime() : null, end25 ? end25.getTime() : null, false);
                
                // Process 2025 YTD (Jan 1 to mirrorToday)
                const ytd25 = processMudancaSheet(sheetDados25, 'All', new Date(2025, 0, 1).getTime(), mirrorToday.getTime(), false);

                metrics25 = {
                    exact: exact25,
                    ytd: ytd25
                };
            }
        }

        return {
            year26: metrics26,
            year25: metrics25
        };

    } catch (e) {
        return { error: e.toString() };
    }
}

function processMudancaSheet(sheet, periodKey, startDate, endDate, kpisOnly) {
    const values = sheet.getDataRange().getValues();
    const dataRows = values.slice(1);
    const durationDisplayValues = sheet.getRange(2, COL_DURACAO + 1, dataRows.length, 1).getDisplayValues();

    const start = startDate ? new Date(startDate) : null;
    const end = endDate ? new Date(endDate) : null;
    if (end) end.setHours(23, 59, 59, 999);

    const initBucket = () => ({
        incidentesTotal: 0, totalDuracaoMinutos: 0,
        sev0Incidentes: 0, sev0DuracaoMinutos: 0,
        sev1Incidentes: 0, sev1DuracaoMinutos: 0,
        monthlyMetrics: {}, weeklyMetrics: {}, quarterlyMetrics: {}
    });

    const buckets = {
        geral: initBucket(),
        deploys: initBucket(),
        tradicionais: initBucket(),
        normais: initBucket(),
        urgente: initBucket(),
        emergencial: initBucket()
    };

    const sheetName = sheet.getName();
    const isMajor26 = sheetName === 'MajorIncidentes2026';

    dataRows.forEach((row, i) => {
        const colMVal = String(row[COL_TECNOLOGIA]).trim().toUpperCase();
        const isTecnologia = colMVal === 'SIM';
        const isConcessao = isMajor26 && colMVal === 'CONCESSÃO';

        const openDate = row[COL_ABERTURA] instanceof Date ? row[COL_ABERTURA] : null;
        if (periodKey === 'T4Industria') {
            const isT4 = String(row[23]).trim() === 'T4 Industria';
            if (!isT4 || (!isTecnologia && !isConcessao)) return;
        } else {
            if (!isTecnologia) return;
            if (start && openDate && openDate < start) return;
            if (end && openDate && openDate > end) return;
        }

        const ofensor = String(row[COL_OFENSOR]).trim();
        if (ofensor !== "Mudança") return;

        const tipoSm = String(row[21]).trim().toUpperCase();
        
        const severidade = String(row[COL_SEVERIDADE]).trim();
        const isSev0 = severidade.startsWith('0');
        const isSev1 = severidade.startsWith('1');

        const durRaw = durationDisplayValues[i] ? durationDisplayValues[i][0] : "00:00";
        const durMin = parseDurationString(durRaw);
        
        const mes = String(row[COL_MES]).trim().toLowerCase();
        const week = openDate ? getWeekLabel(openDate) : "N/A";
        const quarter = openDate ? getQuarterLabel(openDate) : "N/A";

        const addToBucket = (bKey) => {
            const b = buckets[bKey];
            b.incidentesTotal++;
            b.totalDuracaoMinutos += durMin;
            if (isSev0) { b.sev0Incidentes++; b.sev0DuracaoMinutos += durMin; }
            if (isSev1) { b.sev1Incidentes++; b.sev1DuracaoMinutos += durMin; }

            if (!kpisOnly) {
                if (!b.monthlyMetrics[mes]) b.monthlyMetrics[mes] = { count: 0, durationMin: 0 };
                b.monthlyMetrics[mes].count++; b.monthlyMetrics[mes].durationMin += durMin;

                if (!b.weeklyMetrics[week]) b.weeklyMetrics[week] = { count: 0, durationMin: 0 };
                b.weeklyMetrics[week].count++; b.weeklyMetrics[week].durationMin += durMin;

                if (!b.quarterlyMetrics[quarter]) b.quarterlyMetrics[quarter] = { count: 0, durationMin: 0 };
                b.quarterlyMetrics[quarter].count++; b.quarterlyMetrics[quarter].durationMin += durMin;
            }
        };

        addToBucket('geral');
        if (tipoSm === 'DEPLOY') {
            addToBucket('deploys');
        } else if (tipoSm === 'NORMAL') {
            addToBucket('normais');
            addToBucket('tradicionais');
        } else if (tipoSm === 'URGENTE') {
            addToBucket('urgente');
            addToBucket('tradicionais');
        } else if (tipoSm === 'EMERGENCIAL' || tipoSm === 'EMERGENCIA') {
            addToBucket('emergencial');
            addToBucket('tradicionais');
        }
    });

    const calculateMTTRLocal = (total, count) => {
        if (count === 0) return "00:00";
        const avg = total / count;
        const h = Math.floor(avg / 60);
        const m = Math.round(avg % 60);
        return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
    };

    // Format output
    const formatBucket = (b) => {
        const out = {
            kpis: {
                incidentesTotal: b.incidentesTotal,
                mttrTotal: calculateMTTRLocal(b.totalDuracaoMinutos, b.incidentesTotal),
                incidentesSev0: b.sev0Incidentes,
                mttrSev0: calculateMTTRLocal(b.sev0DuracaoMinutos, b.sev0Incidentes),
                incidentesSev1: b.sev1Incidentes,
                mttrSev1: calculateMTTRLocal(b.sev1DuracaoMinutos, b.sev1Incidentes)
            }
        };
        if (!kpisOnly) {
            out.monthlyMetrics = b.monthlyMetrics;
            out.mttrPorMesEmHoras = {};
            for (const mes in b.monthlyMetrics) {
                out.mttrPorMesEmHoras[mes] = b.monthlyMetrics[mes].count > 0 ? (b.monthlyMetrics[mes].durationMin / b.monthlyMetrics[mes].count) / 60 : 0;
            }
            out.weeklyMetrics = b.weeklyMetrics;
            out.mttrSemanalEmHoras = {};
            for (const w in b.weeklyMetrics) {
                out.mttrSemanalEmHoras[w] = b.weeklyMetrics[w].count > 0 ? (b.weeklyMetrics[w].durationMin / b.weeklyMetrics[w].count) / 60 : 0;
            }
            out.quarterlyMetrics = b.quarterlyMetrics;
            out.mttrTrimestralEmHoras = {};
            for (const q in b.quarterlyMetrics) {
                out.mttrTrimestralEmHoras[q] = b.quarterlyMetrics[q].count > 0 ? (b.quarterlyMetrics[q].durationMin / b.quarterlyMetrics[q].count) / 60 : 0;
            }
        }
        return out;
    };

    return {
        geral: formatBucket(buckets.geral),
        deploys: formatBucket(buckets.deploys),
        tradicionais: formatBucket(buckets.tradicionais),
        normais: formatBucket(buckets.normais),
        urgente: formatBucket(buckets.urgente),
        emergencial: formatBucket(buckets.emergencial)
    };
}

/**
 * Coleta Configuração Inicial
 */
function getInitialConfig() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sheetConfig = ss.getSheetByName(SHEET_CONFIG);
 
  let lastUpdated = 'N/A';
  if (sheetConfig) {
    const val = sheetConfig.getRange('B4').getValue();
    lastUpdated = val instanceof Date ? val.toLocaleString('pt-BR') : 'N/A';
  }

  let periodFilters = [];
  if (sheetConfig) {
     const ranges = sheetConfig.getRange('A2:C3').getValues();
     periodFilters = ranges.map(row => ({
        key: String(row[0]).replace(/\s/g, ''),
        start: row[1] instanceof Date ? row[1].getTime() : null,
        end: row[2] instanceof Date ? row[2].getTime() : null
     }));
  }
  periodFilters.unshift({ key: 'All', start: null, end: null });

  // Synthesize "Freeze" Filter (SoftFreeze Start -> HardFreeze End)
  const soft = periodFilters.find(f => f.key.toLowerCase().includes('soft'));
  const hard = periodFilters.find(f => f.key.toLowerCase().includes('hard'));

  if (soft && hard) {
      const freezeFilter = {
          key: 'Freeze',
          start: soft.start,
          end: hard.end
      };
      // Insert after 'All' (index 1)
      periodFilters.splice(1, 0, freezeFilter);
  }

  // Inject T4Industria custom filter
  periodFilters.push({ key: 'T4Industria', start: null, end: null });

  const sheets = ss.getSheets();
  const availableYears = [];
  const regex = /^MajorIncidentes(\d{4})$/i;
 
  sheets.forEach(s => {
    const match = s.getName().match(regex);
    if (match) availableYears.push(match[1]);
  });
 
  availableYears.sort((a, b) => b - a);
  if (availableYears.length === 0) availableYears.push("2025");

  return { lastUpdated, periodFilters, availableYears };
}

/**
 * Busca Dados Filtrados
 */
function getFilteredData(year, selectedPeriodKey, startDate, endDate, selectedCard, techFilter, ofensorFilter) {
  try {
    year = year || new Date().getFullYear();
    selectedPeriodKey = selectedPeriodKey || 'All';
    selectedCard = selectedCard || 'total';
    const techFilterSet = (techFilter && techFilter.length) ? new Set(techFilter) : null;
    const ofensorFilterSet = (ofensorFilter && ofensorFilter.length) ? new Set(ofensorFilter) : null;

    const targetSheetName = `MajorIncidentes${year}`;
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const sheetDados = ss.getSheetByName(targetSheetName);
   
    if (!sheetDados) {
        return { error: `Dados para o ano ${year} não encontrados.` };
    }
   
    const values = sheetDados.getDataRange().getValues();
    const dataRows = values.slice(1);
    if (dataRows.length === 0) {
        // Return initialized empty metrics to avoid frontend crash
        return {
           kpis: {
             incidentesTotal: 0, mttrTotal: "00:00",
             incidentesSev0: 0, mttrSev0: "00:00",
             incidentesSev1: 0, mttrSev1: "00:00",
             aderenciaOLA: 0, aderenciaOLABase: 0,
             aderenciaOLASev0: 0, aderenciaOLASev1: 0,
             incidentesMudanca: 0, pctMudanca: 0,
             mttdInicioMedioHoras: null, mttdTerminoMedioHoras: null,
             incidentesMudancaDeploy: 0, incidentesMudancaTradicional: 0,
             mttdInicioMedioHorasDeploy: null, mttdInicioMedioHorasTradicional: null,
             aderenciaSevPrio: null, aderenciaSevPrioBase: 0,
             aderenciaSevPrioSev0: null, aderenciaSevPrioSev0Base: 0,
             aderenciaSevPrioSev1: null, aderenciaSevPrioSev1Base: 0,
             divergenciasSevPrioCount: 0,
             mttdTerminoMedioHorasDeploy: null, mttdTerminoMedioHorasTradicional: null
           },
           divergenciasSevPrio: [],
           monthlyMetrics: {},
           mttrPorMesEmHoras: {},
           techMetrics: {},
           offenderMetrics: {},
           jornadaMetrics: {},
           paisMetrics: {},
           origemDeteccao: {
             'End-users': { count: 0, mttr: "00:00" },
             'Monitoração': { count: 0, mttr: "00:00" },
             'Experiências': { count: 0, mttr: "00:00" },
             base: 0
           },
           gruposResponsaveis: { deploy: [], tradicional: [] },
           sankeyMudanca: { deploy: [], tradicional: [] },
           mttdVsMttrDispersao: [],
           mttdTerminoVsMttrDispersao: [],
           filterOptions: { tecnologias: [], ofensores: [] },
           monthlyBySeveridade: {}, weeklyBySeveridade: {}, quarterlyBySeveridade: {},
           monthlyByOrigem: {}, weeklyByOrigem: {}, quarterlyByOrigem: {},
           monthlyByJornada: {}, weeklyByJornada: {}, quarterlyByJornada: {},
           monthlyByPais: {}, weeklyByPais: {}, quarterlyByPais: {},
           mudancaEvolutionData: {
             monthly: { metrics: {}, mttr: {} },
             weekly: { metrics: {}, mttr: {} },
             quarterly: { metrics: {}, mttr: {} }
           }
        };
    }

    const durationDisplayValues = sheetDados.getRange(2, COL_DURACAO + 1, dataRows.length, 1).getDisplayValues();

    const start = startDate ? new Date(startDate) : null;
    const end = endDate ? new Date(endDate) : null;
    if (end) end.setHours(23, 59, 59, 999);

    const changeMap = buildChangeMap(ss);
    const priorityMap = buildPriorityMap(ss);
    const callerMap = buildCallerMap(ss);

    let metrics = {
        incidentesTotal: 0, totalDuracaoMinutos: 0,
        sev0Incidentes: 0, sev0DuracaoMinutos: 0,
        sev1Incidentes: 0, sev1DuracaoMinutos: 0,
        monthlyMetrics: {},
        weeklyMetrics: {},
        quarterlyMetrics: {},
        techMetrics: {},
        offenderMetrics: {},
        jornadaMetrics: {},
        paisMetrics: {},
        rawIncidents: [],
        // Visão Executiva: Mudança & OLA
        sev0DentroOLA: 0, sev1DentroOLA: 0,
        incidentesMudanca: 0,
        mttdInicioSomaHoras: 0, mttdInicioCount: 0,
        mttdTerminoSomaHoras: 0, mttdTerminoCount: 0,
        mudancaPorTipo: {
            deploy: { count: 0, mttdInicioSoma: 0, mttdInicioCount: 0, mttdTerminoSoma: 0, mttdTerminoCount: 0 },
            tradicional: { count: 0, mttdInicioSoma: 0, mttdInicioCount: 0, mttdTerminoSoma: 0, mttdTerminoCount: 0 }
        },
        mttdVsMttrDispersao: [],
        mttdTerminoVsMttrDispersao: [],
        // Governança: Aderência Sev x Prioridade
        sevPrioBase: 0, sevPrioAderente: 0,
        sev0PrioBase: 0, sev0PrioAderente: 0,
        sev1PrioBase: 0, sev1PrioAderente: 0,
        divergenciasSevPrio: [],
        // Origem da Detecção (Caller do ServiceNow)
        origemDeteccao: {
            'End-users': { count: 0, durationMin: 0 },
            'Monitoração': { count: 0, durationMin: 0 },
            'Experiências': { count: 0, durationMin: 0 }
        },
        origemDeteccaoBase: 0,
        // Top Grupos/Serviços Responsáveis por Incidentes causados por Mudança (com detalhe por Tecnologia, p/ Sankey)
        gruposDeploy: {},       // key: serviceOffering -> { count, tecnologias: {tech: count} }
        gruposTradicional: {},  // key: grupo + '||' + chgService -> { grupo, chgService, count, tecnologias: {tech: count} }
        // Filtros disponíveis (para popular os selects de Tecnologia/Ofensor)
        tecnologiasDisponiveis: new Set(),
        ofensoresDisponiveis: new Set(),
        // Evolução por Dimensão (Severidade / Origem da Detecção / Jornada / País), nos 3 agrupamentos temporais
        monthlyBySeveridade: {}, weeklyBySeveridade: {}, quarterlyBySeveridade: {},
        monthlyByOrigem: {}, weeklyByOrigem: {}, quarterlyByOrigem: {},
        monthlyByJornada: {}, weeklyByJornada: {}, quarterlyByJornada: {},
        monthlyByPais: {}, weeklyByPais: {}, quarterlyByPais: {},
        // Evolução (Volume + MTTR) restrita a Incidentes causados por Mudança
        mudancaMonthlyMetrics: {}, mudancaWeeklyMetrics: {}, mudancaQuarterlyMetrics: {}
    };

    dataRows.forEach((row, i) => {
        const colMVal = String(row[COL_TECNOLOGIA]).trim().toUpperCase();
        const isTecnologia = colMVal === 'SIM';
        const isConcessao = (targetSheetName === 'MajorIncidentes2026') && colMVal === 'CONCESSÃO';
        
        // 2. Filtro de Data ou Filtro T4 Industria
        const openDate = row[COL_ABERTURA] instanceof Date ? row[COL_ABERTURA] : null;
        
        if (selectedPeriodKey === 'T4Industria') {
            const isT4 = String(row[23]).trim() === 'T4 Industria';
            if (!isT4 || (!isTecnologia && !isConcessao)) return;
        } else {
            if (!isTecnologia) return;
            if (start && openDate && openDate < start) return;
            if (end && openDate && openDate > end) return;
        }
        
        const severidade = String(row[COL_SEVERIDADE]).trim();
        let isSev0 = severidade.startsWith('0');
        let isSev1 = severidade.startsWith('1');

        if (selectedCard === 'sev0' && !isSev0) return;
        if (selectedCard === 'sev1' && !isSev1) return;
        
        const durRaw = durationDisplayValues[i] ? durationDisplayValues[i][0] : "00:00";
        const durMin = parseDurationString(durRaw);
        const mes = String(row[COL_MES]).trim().toLowerCase();
        
        const techImpactada = String(row[COL_TECH_IMPACTADA]).trim() || "N/A";
        const ofensor = String(row[COL_OFENSOR]).trim() || "N/A";

        // Coleta as opções disponíveis para os filtros de Tecnologia/Ofensor (antes de aplicá-los)
        metrics.tecnologiasDisponiveis.add(techImpactada);
        metrics.ofensoresDisponiveis.add(ofensor);

        // Filtro por Tecnologia Impactada / Ofensor (multi-seleção)
        if (techFilterSet && !techFilterSet.has(techImpactada)) return;
        if (ofensorFilterSet && !ofensorFilterSet.has(ofensor)) return;

        // KPIs (agora com filtro Tecnologia = SIM applied)
        metrics.incidentesTotal++;
        metrics.totalDuracaoMinutos += durMin;
        
        if (!metrics.monthlyMetrics[mes]) metrics.monthlyMetrics[mes] = { count: 0, durationMin: 0 };
        metrics.monthlyMetrics[mes].count++;
        metrics.monthlyMetrics[mes].durationMin += durMin;

        // Weekly & Quarterly
        let weekLabel = null, quarterLabel = null;
        if (openDate) {
          weekLabel = getWeekLabel(openDate);
          if (!metrics.weeklyMetrics[weekLabel]) metrics.weeklyMetrics[weekLabel] = { count: 0, durationMin: 0 };
          metrics.weeklyMetrics[weekLabel].count++;
          metrics.weeklyMetrics[weekLabel].durationMin += durMin;

          quarterLabel = getQuarterLabel(openDate);
          if (!metrics.quarterlyMetrics[quarterLabel]) metrics.quarterlyMetrics[quarterLabel] = { count: 0, durationMin: 0 };
          metrics.quarterlyMetrics[quarterLabel].count++;
          metrics.quarterlyMetrics[quarterLabel].durationMin += durMin;
        }

        // Evolução por Dimensão (Severidade/Detecção/Jornada/País): grava no período Mensal, Semanal e Trimestral
        const bumpBreakdown = (bucket, periodLabel, key) => {
            if (!periodLabel) return;
            if (!bucket[periodLabel]) bucket[periodLabel] = {};
            bucket[periodLabel][key] = (bucket[periodLabel][key] || 0) + 1;
        };

        if (!metrics.monthlyBySeveridade[mes]) metrics.monthlyBySeveridade[mes] = { sev0: 0, sev1: 0 };
        if (isSev0) {
        metrics.sev0Incidentes++;
        metrics.sev0DuracaoMinutos += durMin;
        if (durMin <= OLA_TARGET_SEV0_MIN) metrics.sev0DentroOLA++;
        metrics.monthlyBySeveridade[mes].sev0++;
        bumpBreakdown(metrics.weeklyBySeveridade, weekLabel, 'sev0');
        bumpBreakdown(metrics.quarterlyBySeveridade, quarterLabel, 'sev0');
        } else if (isSev1) {
        metrics.sev1Incidentes++;
        metrics.sev1DuracaoMinutos += durMin;
        if (durMin <= OLA_TARGET_SEV1_MIN) metrics.sev1DentroOLA++;
        metrics.monthlyBySeveridade[mes].sev1++;
        bumpBreakdown(metrics.weeklyBySeveridade, weekLabel, 'sev1');
        bumpBreakdown(metrics.quarterlyBySeveridade, quarterLabel, 'sev1');
        }

        // Governança: Aderência Sev x Prioridade (Sev0/Sev1 são aderentes se Priority = P1-Critical ou P2-High)
        if (isSev0 || isSev1) {
            const ticketId = String(row[0] || '').trim();
            const priority = ticketId ? priorityMap[ticketId] : undefined;
            if (priority) {
                metrics.sevPrioBase++;
                const prioTrim = priority.trim();
                const aderente = prioTrim.startsWith('1') || prioTrim.startsWith('2');
                if (isSev0) {
                    metrics.sev0PrioBase++;
                    if (aderente) metrics.sev0PrioAderente++;
                } else {
                    metrics.sev1PrioBase++;
                    if (aderente) metrics.sev1PrioAderente++;
                }
                if (aderente) {
                    metrics.sevPrioAderente++;
                } else {
                    metrics.divergenciasSevPrio.push({
                        id: ticketId,
                        severidade: severidade,
                        priority: priority
                    });
                }
            }
        }

        // Origem da Detecção do Incidente (Caller do ServiceNow): End-users / Monitoração / Experiências
        if (isSev0 || isSev1) {
            const ticketIdCaller = String(row[0] || '').trim();
            const caller = ticketIdCaller ? callerMap[ticketIdCaller] : undefined;
            const origem = classifyCaller(caller);
            if (origem) {
                metrics.origemDeteccao[origem].count++;
                metrics.origemDeteccao[origem].durationMin += durMin;
                metrics.origemDeteccaoBase++;

                if (!metrics.monthlyByOrigem[mes]) metrics.monthlyByOrigem[mes] = { 'End-users': 0, 'Monitoração': 0, 'Experiências': 0 };
                metrics.monthlyByOrigem[mes][origem]++;
                bumpBreakdown(metrics.weeklyByOrigem, weekLabel, origem);
                bumpBreakdown(metrics.quarterlyByOrigem, quarterLabel, origem);
            }
        }

        // Jornadas Impactadas (coluna F): uma célula pode conter mais de uma Jornada, separadas por vírgula
        const jornadaRaw = String(row[COL_JORNADA] || '').trim();
        if (jornadaRaw) {
            jornadaRaw.split(',').map(j => j.trim()).filter(Boolean).forEach(jornada => {
                if (!metrics.jornadaMetrics[jornada]) metrics.jornadaMetrics[jornada] = { count: 0, durationMin: 0 };
                metrics.jornadaMetrics[jornada].count++;
                metrics.jornadaMetrics[jornada].durationMin += durMin;

                if (!metrics.monthlyByJornada[mes]) metrics.monthlyByJornada[mes] = {};
                metrics.monthlyByJornada[mes][jornada] = (metrics.monthlyByJornada[mes][jornada] || 0) + 1;
                bumpBreakdown(metrics.weeklyByJornada, weekLabel, jornada);
                bumpBreakdown(metrics.quarterlyByJornada, quarterLabel, jornada);
            });
        }

        // Países Impactados (coluna J - Abrangência): códigos separados por vírgula (ex: BR, AR, CO)
        const paisRaw = String(row[COL_ABRANGENCIA] || '').trim();
        if (paisRaw) {
            paisRaw.split(',').map(p => p.trim().toUpperCase()).filter(Boolean).forEach(pais => {
                if (!metrics.paisMetrics[pais]) metrics.paisMetrics[pais] = { count: 0, durationMin: 0 };
                metrics.paisMetrics[pais].count++;
                metrics.paisMetrics[pais].durationMin += durMin;

                if (!metrics.monthlyByPais[mes]) metrics.monthlyByPais[mes] = {};
                metrics.monthlyByPais[mes][pais] = (metrics.monthlyByPais[mes][pais] || 0) + 1;
                bumpBreakdown(metrics.weeklyByPais, weekLabel, pais);
                bumpBreakdown(metrics.quarterlyByPais, quarterLabel, pais);
            });
        }

        // Visão Executiva: Incidentes causados por Mudança + MTTD (Início/Término), segmentado por tipo
        if (ofensor === 'Mudança') {
            metrics.incidentesMudanca++;

            // Evolução (Mensal/Semanal/Trimestral) de Volume + MTTR restrita a Incidentes causados por Mudança
            if (!metrics.mudancaMonthlyMetrics[mes]) metrics.mudancaMonthlyMetrics[mes] = { count: 0, durationMin: 0 };
            metrics.mudancaMonthlyMetrics[mes].count++;
            metrics.mudancaMonthlyMetrics[mes].durationMin += durMin;
            if (weekLabel) {
                if (!metrics.mudancaWeeklyMetrics[weekLabel]) metrics.mudancaWeeklyMetrics[weekLabel] = { count: 0, durationMin: 0 };
                metrics.mudancaWeeklyMetrics[weekLabel].count++;
                metrics.mudancaWeeklyMetrics[weekLabel].durationMin += durMin;
            }
            if (quarterLabel) {
                if (!metrics.mudancaQuarterlyMetrics[quarterLabel]) metrics.mudancaQuarterlyMetrics[quarterLabel] = { count: 0, durationMin: 0 };
                metrics.mudancaQuarterlyMetrics[quarterLabel].count++;
                metrics.mudancaQuarterlyMetrics[quarterLabel].durationMin += durMin;
            }

            const tipoSm = String(row[COL_TYPE_SM]).trim().toUpperCase();
            const isDeploy = tipoSm === 'DEPLOY';
            const tipoBucket = isDeploy ? 'deploy' : 'tradicional';
            metrics.mudancaPorTipo[tipoBucket].count++;

            const numSm = String(row[COL_SM_NUMBER]).trim();
            const chg = numSm ? changeMap[numSm] : null;

            // Top Grupos/Serviços Responsáveis (contagem por Tecnologia, usada no ranking e no Diagrama de Sankey)
            if (chg) {
                if (isDeploy) {
                    const key = chg.serviceOffering || 'N/A';
                    if (!metrics.gruposDeploy[key]) metrics.gruposDeploy[key] = { count: 0, tecnologias: {} };
                    metrics.gruposDeploy[key].count++;
                    metrics.gruposDeploy[key].tecnologias[techImpactada] = (metrics.gruposDeploy[key].tecnologias[techImpactada] || 0) + 1;
                } else {
                    const key = chg.assignmentGroup + '||' + chg.service;
                    if (!metrics.gruposTradicional[key]) {
                        metrics.gruposTradicional[key] = { grupo: chg.assignmentGroup, chgService: chg.service, count: 0, tecnologias: {} };
                    }
                    metrics.gruposTradicional[key].count++;
                    metrics.gruposTradicional[key].tecnologias[techImpactada] = (metrics.gruposTradicional[key].tecnologias[techImpactada] || 0) + 1;
                }
            }

            if (chg && openDate) {
                if (chg.plannedStart) {
                    const diffInicioH = (openDate.getTime() - chg.plannedStart.getTime()) / (1000 * 60 * 60);
                    if (diffInicioH >= 0) {
                        metrics.mttdInicioSomaHoras += diffInicioH;
                        metrics.mttdInicioCount++;
                        metrics.mudancaPorTipo[tipoBucket].mttdInicioSoma += diffInicioH;
                        metrics.mudancaPorTipo[tipoBucket].mttdInicioCount++;

                        // Dispersão MTTD (Início) x MTTR, por Incidente causado por Mudança
                        metrics.mttdVsMttrDispersao.push({
                            id: String(row[0] || '').trim(),
                            mttdHoras: Math.round(diffInicioH * 10) / 10,
                            mttrHoras: Math.round((durMin / 60) * 10) / 10,
                            tipo: tipoBucket
                        });
                    }
                }
                if (chg.plannedEnd) {
                    const diffTerminoH = (openDate.getTime() - chg.plannedEnd.getTime()) / (1000 * 60 * 60);
                    if (diffTerminoH >= 0) {
                        metrics.mttdTerminoSomaHoras += diffTerminoH;
                        metrics.mttdTerminoCount++;
                        metrics.mudancaPorTipo[tipoBucket].mttdTerminoSoma += diffTerminoH;
                        metrics.mudancaPorTipo[tipoBucket].mttdTerminoCount++;

                        // Dispersão MTTD (Término) x MTTR, por Incidente causado por Mudança
                        metrics.mttdTerminoVsMttrDispersao.push({
                            id: String(row[0] || '').trim(),
                            mttdHoras: Math.round(diffTerminoH * 10) / 10,
                            mttrHoras: Math.round((durMin / 60) * 10) / 10,
                            tipo: tipoBucket
                        });
                    }
                }
            }
        }


        // Tech Aggregation (isTecnologia já foi verificado no início do loop)
        if (!metrics.techMetrics[techImpactada]) {
            metrics.techMetrics[techImpactada] = { count: 0, durationMin: 0, breakdown: {} };
        }
        metrics.techMetrics[techImpactada].count++;
        metrics.techMetrics[techImpactada].durationMin += durMin;
        
        if (!metrics.techMetrics[techImpactada].breakdown[ofensor]) {
            metrics.techMetrics[techImpactada].breakdown[ofensor] = { count: 0, durationMin: 0 };
        }
        metrics.techMetrics[techImpactada].breakdown[ofensor].count++;
        metrics.techMetrics[techImpactada].breakdown[ofensor].durationMin += durMin;

        // Offender Aggregation
        if (!metrics.offenderMetrics[ofensor]) {
            metrics.offenderMetrics[ofensor] = { count: 0, durationMin: 0, breakdown: {} };
        }
        metrics.offenderMetrics[ofensor].count++;
        metrics.offenderMetrics[ofensor].durationMin += durMin;

        if (!metrics.offenderMetrics[ofensor].breakdown[techImpactada]) {
            metrics.offenderMetrics[ofensor].breakdown[techImpactada] = { count: 0, durationMin: 0 };
        }
        metrics.offenderMetrics[ofensor].breakdown[techImpactada].count++;
        metrics.offenderMetrics[ofensor].breakdown[techImpactada].durationMin += durMin;

        const pad = n => n.toString().padStart(2, '0');
        const formatDate = (val) => {
            if (val instanceof Date) {
                return `${pad(val.getDate())}/${pad(val.getMonth()+1)}/${val.getFullYear()} ${pad(val.getHours())}:${pad(val.getMinutes())}`;
            }
            return val ? String(val) : "N/A";
        };

        metrics.rawIncidents.push({
            id: String(row[0]).trim() || "N/A",
            problem: String(row[COL_PROBLEMA] || "").trim(),
            severidade: severidade,
            abrangencia: String(row[9]).trim() || "N/A",
            descImpacto: String(row[7]).trim() || "N/A",
            descOfensor: String(row[10]).trim() || "N/A",
            solucao: String(row[11]).trim() || "N/A",
            dataDefinicao: formatDate(row[2]),
            dataAberturaTimestamp: (row[2] instanceof Date) ? row[2].getTime() : 0,
            dataEncerramento: formatDate(row[3]),
            ttr: durRaw,
            jornada: String(row[5]).trim() || "N/A",
            tipoSm: String(row[21]).trim() || "N/A",
            numSm: String(row[22]).trim() || "N/A",
            mes: mes,
            weekLabel: openDate ? getWeekLabel(openDate) : "N/A",
            quarterLabel: openDate ? getQuarterLabel(openDate) : "N/A",
            tech: techImpactada,
            offender: ofensor,
            consideracoes: String(row[24] || "").trim(),
            isConcessao: isConcessao
        });
    });

    const calculateMTTR = (total, count) => {
        if (count === 0) return "00:00";
        const avg = total / count;
        const h = Math.floor(avg / 60);
        const m = Math.round(avg % 60);
        return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
    };

    const mttrPorMesEmHoras = {};
    for (const mes in metrics.monthlyMetrics) {
        const item = metrics.monthlyMetrics[mes];
        mttrPorMesEmHoras[mes] = item.count > 0 ? (item.durationMin / item.count) / 60 : 0;
    }

    const mttrSemanalEmHoras = {};
    for (const week in metrics.weeklyMetrics) {
        const item = metrics.weeklyMetrics[week];
        mttrSemanalEmHoras[week] = item.count > 0 ? (item.durationMin / item.count) / 60 : 0;
    }

    const mttrTrimestralEmHoras = {};
    for (const quarter in metrics.quarterlyMetrics) {
        const item = metrics.quarterlyMetrics[quarter];
        mttrTrimestralEmHoras[quarter] = item.count > 0 ? (item.durationMin / item.count) / 60 : 0;
    }

    // MTTR (Mensal/Semanal/Trimestral) restrito a Incidentes causados por Mudança
    const mudancaMttrPorMesEmHoras = {};
    for (const mes in metrics.mudancaMonthlyMetrics) {
        const item = metrics.mudancaMonthlyMetrics[mes];
        mudancaMttrPorMesEmHoras[mes] = item.count > 0 ? (item.durationMin / item.count) / 60 : 0;
    }
    const mudancaMttrSemanalEmHoras = {};
    for (const week in metrics.mudancaWeeklyMetrics) {
        const item = metrics.mudancaWeeklyMetrics[week];
        mudancaMttrSemanalEmHoras[week] = item.count > 0 ? (item.durationMin / item.count) / 60 : 0;
    }
    const mudancaMttrTrimestralEmHoras = {};
    for (const quarter in metrics.mudancaQuarterlyMetrics) {
        const item = metrics.mudancaQuarterlyMetrics[quarter];
        mudancaMttrTrimestralEmHoras[quarter] = item.count > 0 ? (item.durationMin / item.count) / 60 : 0;
    }

    const sev0Sev1Total = metrics.sev0Incidentes + metrics.sev1Incidentes;
    const dentroOLATotal = metrics.sev0DentroOLA + metrics.sev1DentroOLA;
    const aderenciaOLA = sev0Sev1Total > 0 ? (dentroOLATotal / sev0Sev1Total) * 100 : 0;
    const aderenciaOLASev0 = metrics.sev0Incidentes > 0 ? (metrics.sev0DentroOLA / metrics.sev0Incidentes) * 100 : 0;
    const aderenciaOLASev1 = metrics.sev1Incidentes > 0 ? (metrics.sev1DentroOLA / metrics.sev1Incidentes) * 100 : 0;
    const pctMudanca = metrics.incidentesTotal > 0 ? (metrics.incidentesMudanca / metrics.incidentesTotal) * 100 : 0;
    const mttdInicioMedio = metrics.mttdInicioCount > 0 ? metrics.mttdInicioSomaHoras / metrics.mttdInicioCount : null;
    const mttdTerminoMedio = metrics.mttdTerminoCount > 0 ? metrics.mttdTerminoSomaHoras / metrics.mttdTerminoCount : null;

    const deployBucket = metrics.mudancaPorTipo.deploy;
    const tradicionalBucket = metrics.mudancaPorTipo.tradicional;
    const mttdInicioDeploy = deployBucket.mttdInicioCount > 0 ? deployBucket.mttdInicioSoma / deployBucket.mttdInicioCount : null;
    const mttdInicioTradicional = tradicionalBucket.mttdInicioCount > 0 ? tradicionalBucket.mttdInicioSoma / tradicionalBucket.mttdInicioCount : null;
    const mttdTerminoDeploy = deployBucket.mttdTerminoCount > 0 ? deployBucket.mttdTerminoSoma / deployBucket.mttdTerminoCount : null;
    const mttdTerminoTradicional = tradicionalBucket.mttdTerminoCount > 0 ? tradicionalBucket.mttdTerminoSoma / tradicionalBucket.mttdTerminoCount : null;

    const aderenciaSevPrio = metrics.sevPrioBase > 0 ? (metrics.sevPrioAderente / metrics.sevPrioBase) * 100 : null;
    const aderenciaSevPrioSev0 = metrics.sev0PrioBase > 0 ? (metrics.sev0PrioAderente / metrics.sev0PrioBase) * 100 : null;
    const aderenciaSevPrioSev1 = metrics.sev1PrioBase > 0 ? (metrics.sev1PrioAderente / metrics.sev1PrioBase) * 100 : null;

    // Top Grupos Responsáveis: converte contagens por Tecnologia em arrays e ordena por Quantidade (desc)
    const gruposDeployList = Object.keys(metrics.gruposDeploy).map(key => ({
        serviceOffering: key,
        count: metrics.gruposDeploy[key].count,
        tecnologias: Object.keys(metrics.gruposDeploy[key].tecnologias)
    })).sort((a, b) => b.count - a.count);

    const gruposTradicionalList = Object.keys(metrics.gruposTradicional).map(key => {
        const g = metrics.gruposTradicional[key];
        return {
            grupo: g.grupo,
            chgService: g.chgService,
            count: g.count,
            tecnologias: Object.keys(g.tecnologias)
        };
    }).sort((a, b) => b.count - a.count);

    // Diagrama de Sankey: Deploy é 2 estágios (Service Offering -> Tecnologia).
    const sankeyDeploy = [];
    Object.keys(metrics.gruposDeploy).forEach(key => {
        const techs = metrics.gruposDeploy[key].tecnologias;
        Object.keys(techs).forEach(tech => {
            sankeyDeploy.push({ from: key, to: tech, flow: techs[tech] });
        });
    });

    // Tradicional é 3 estágios (Grupo -> CHG Service -> Tecnologia), agregando por par em cada estágio.
    const grupoServiceCounts = {};
    const serviceTechCounts = {};
    Object.keys(metrics.gruposTradicional).forEach(key => {
        const g = metrics.gruposTradicional[key];
        const stage1Key = g.grupo + '||' + g.chgService;
        if (!grupoServiceCounts[stage1Key]) grupoServiceCounts[stage1Key] = { from: g.grupo, to: g.chgService, flow: 0 };
        grupoServiceCounts[stage1Key].flow += g.count;

        Object.keys(g.tecnologias).forEach(tech => {
            const stage2Key = g.chgService + '||' + tech;
            if (!serviceTechCounts[stage2Key]) serviceTechCounts[stage2Key] = { from: g.chgService, to: tech, flow: 0 };
            serviceTechCounts[stage2Key].flow += g.tecnologias[tech];
        });
    });
    const sankeyTradicional = Object.keys(grupoServiceCounts).map(k => grupoServiceCounts[k])
        .concat(Object.keys(serviceTechCounts).map(k => serviceTechCounts[k]));

    // Origem da Detecção: formata contagem + MTTR por origem
    const origemDeteccaoFormatted = {};
    ['End-users', 'Monitoração', 'Experiências'].forEach(k => {
        const item = metrics.origemDeteccao[k];
        origemDeteccaoFormatted[k] = {
            count: item.count,
            mttr: calculateMTTR(item.durationMin, item.count)
        };
    });

    return {
        kpis: {
        incidentesTotal: metrics.incidentesTotal,
        mttrTotal: calculateMTTR(metrics.totalDuracaoMinutos, metrics.incidentesTotal),
        incidentesSev0: metrics.sev0Incidentes,
        mttrSev0: calculateMTTR(metrics.sev0DuracaoMinutos, metrics.sev0Incidentes),
        incidentesSev1: metrics.sev1Incidentes,
        mttrSev1: calculateMTTR(metrics.sev1DuracaoMinutos, metrics.sev1Incidentes),
        // Visão Executiva
        aderenciaOLA: Math.round(aderenciaOLA * 10) / 10,
        aderenciaOLABase: sev0Sev1Total,
        aderenciaOLASev0: Math.round(aderenciaOLASev0 * 10) / 10,
        aderenciaOLASev1: Math.round(aderenciaOLASev1 * 10) / 10,
        incidentesMudanca: metrics.incidentesMudanca,
        pctMudanca: Math.round(pctMudanca * 10) / 10,
        mttdInicioMedioHoras: mttdInicioMedio !== null ? Math.round(mttdInicioMedio * 10) / 10 : null,
        mttdTerminoMedioHoras: mttdTerminoMedio !== null ? Math.round(mttdTerminoMedio * 10) / 10 : null,
        incidentesMudancaDeploy: deployBucket.count,
        incidentesMudancaTradicional: tradicionalBucket.count,
        mttdInicioMedioHorasDeploy: mttdInicioDeploy !== null ? Math.round(mttdInicioDeploy * 10) / 10 : null,
        mttdInicioMedioHorasTradicional: mttdInicioTradicional !== null ? Math.round(mttdInicioTradicional * 10) / 10 : null,
        mttdTerminoMedioHorasDeploy: mttdTerminoDeploy !== null ? Math.round(mttdTerminoDeploy * 10) / 10 : null,
        mttdTerminoMedioHorasTradicional: mttdTerminoTradicional !== null ? Math.round(mttdTerminoTradicional * 10) / 10 : null,
        // Governança: Aderência Sev x Prioridade
        aderenciaSevPrio: aderenciaSevPrio !== null ? Math.round(aderenciaSevPrio * 10) / 10 : null,
        aderenciaSevPrioBase: metrics.sevPrioBase,
        aderenciaSevPrioSev0: aderenciaSevPrioSev0 !== null ? Math.round(aderenciaSevPrioSev0 * 10) / 10 : null,
        aderenciaSevPrioSev0Base: metrics.sev0PrioBase,
        aderenciaSevPrioSev1: aderenciaSevPrioSev1 !== null ? Math.round(aderenciaSevPrioSev1 * 10) / 10 : null,
        aderenciaSevPrioSev1Base: metrics.sev1PrioBase,
        divergenciasSevPrioCount: metrics.divergenciasSevPrio.length
        },
        divergenciasSevPrio: metrics.divergenciasSevPrio,
        monthlyMetrics: metrics.monthlyMetrics,
        mttrPorMesEmHoras: mttrPorMesEmHoras,
        weeklyMetrics: metrics.weeklyMetrics,
        mttrSemanalEmHoras: mttrSemanalEmHoras,
        quarterlyMetrics: metrics.quarterlyMetrics,
        mttrTrimestralEmHoras: mttrTrimestralEmHoras,
        techMetrics: metrics.techMetrics,
        offenderMetrics: metrics.offenderMetrics,
        jornadaMetrics: metrics.jornadaMetrics,
        paisMetrics: metrics.paisMetrics,
        origemDeteccao: {
            'End-users': origemDeteccaoFormatted['End-users'],
            'Monitoração': origemDeteccaoFormatted['Monitoração'],
            'Experiências': origemDeteccaoFormatted['Experiências'],
            base: metrics.origemDeteccaoBase
        },
        gruposResponsaveis: {
            deploy: gruposDeployList,
            tradicional: gruposTradicionalList
        },
        sankeyMudanca: {
            deploy: sankeyDeploy,
            tradicional: sankeyTradicional
        },
        mttdVsMttrDispersao: metrics.mttdVsMttrDispersao,
        mttdTerminoVsMttrDispersao: metrics.mttdTerminoVsMttrDispersao,
        filterOptions: {
            tecnologias: Array.from(metrics.tecnologiasDisponiveis).sort(),
            ofensores: Array.from(metrics.ofensoresDisponiveis).sort()
        },
        monthlyBySeveridade: metrics.monthlyBySeveridade,
        weeklyBySeveridade: metrics.weeklyBySeveridade,
        quarterlyBySeveridade: metrics.quarterlyBySeveridade,
        monthlyByOrigem: metrics.monthlyByOrigem,
        weeklyByOrigem: metrics.weeklyByOrigem,
        quarterlyByOrigem: metrics.quarterlyByOrigem,
        monthlyByJornada: metrics.monthlyByJornada,
        weeklyByJornada: metrics.weeklyByJornada,
        quarterlyByJornada: metrics.quarterlyByJornada,
        monthlyByPais: metrics.monthlyByPais,
        weeklyByPais: metrics.weeklyByPais,
        quarterlyByPais: metrics.quarterlyByPais,
        mudancaEvolutionData: {
            monthly: { metrics: metrics.mudancaMonthlyMetrics, mttr: mudancaMttrPorMesEmHoras },
            weekly: { metrics: metrics.mudancaWeeklyMetrics, mttr: mudancaMttrSemanalEmHoras },
            quarterly: { metrics: metrics.mudancaQuarterlyMetrics, mttr: mudancaMttrTrimestralEmHoras }
        },
        rawIncidents: metrics.rawIncidents
    };
  } catch (e) {
      return { error: "Erro no servidor: " + e.message };
  }
}

/**
 * Versão enxuta de getFilteredData: calcula apenas os KPIs essenciais (Total, Sev0, Sev1, Aderência OLA)
 * para um intervalo de datas, usada nas comparações "vs período anterior".
 */
function getPeriodKpisOnly(year, startDate, endDate) {
    const targetSheetName = `MajorIncidentes${year}`;
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const sheetDados = ss.getSheetByName(targetSheetName);
    if (!sheetDados) return null;

    const values = sheetDados.getDataRange().getValues();
    const dataRows = values.slice(1);

    const calcMTTR = (t, c) => {
        if (c === 0) return "00:00";
        const avg = t / c;
        const h = Math.floor(avg / 60);
        const m = Math.round(avg % 60);
        return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
    };

    if (dataRows.length === 0) {
        return {
            incidentesTotal: 0, mttrTotal: "00:00",
            incidentesSev0: 0, mttrSev0: "00:00",
            incidentesSev1: 0, mttrSev1: "00:00",
            aderenciaOLA: 0, aderenciaOLASev0: 0, aderenciaOLASev1: 0
        };
    }

    const durationDisplayValues = sheetDados.getRange(2, COL_DURACAO + 1, dataRows.length, 1).getDisplayValues();
    const start = startDate ? new Date(startDate) : null;
    const end = endDate ? new Date(endDate) : null;
    if (end) end.setHours(23, 59, 59, 999);

    let total = 0, totalDur = 0, sev0 = 0, sev0Dur = 0, sev1 = 0, sev1Dur = 0, sev0OLA = 0, sev1OLA = 0;

    dataRows.forEach((row, i) => {
        const colMVal = String(row[COL_TECNOLOGIA]).trim().toUpperCase();
        const isTecnologia = colMVal === 'SIM';
        const openDate = row[COL_ABERTURA] instanceof Date ? row[COL_ABERTURA] : null;
        if (!isTecnologia) return;
        if (start && openDate && openDate < start) return;
        if (end && openDate && openDate > end) return;

        const severidade = String(row[COL_SEVERIDADE]).trim();
        const isSev0 = severidade.startsWith('0');
        const isSev1 = severidade.startsWith('1');

        const durRaw = durationDisplayValues[i] ? durationDisplayValues[i][0] : "00:00";
        const durMin = parseDurationString(durRaw);

        total++;
        totalDur += durMin;
        if (isSev0) {
            sev0++; sev0Dur += durMin;
            if (durMin <= OLA_TARGET_SEV0_MIN) sev0OLA++;
        } else if (isSev1) {
            sev1++; sev1Dur += durMin;
            if (durMin <= OLA_TARGET_SEV1_MIN) sev1OLA++;
        }
    });

    const sev0Sev1Total = sev0 + sev1;
    const dentroOLATotal = sev0OLA + sev1OLA;

    return {
        incidentesTotal: total,
        mttrTotal: calcMTTR(totalDur, total),
        incidentesSev0: sev0,
        mttrSev0: calcMTTR(sev0Dur, sev0),
        incidentesSev1: sev1,
        mttrSev1: calcMTTR(sev1Dur, sev1),
        aderenciaOLA: sev0Sev1Total > 0 ? Math.round((dentroOLATotal / sev0Sev1Total) * 1000) / 10 : 0,
        aderenciaOLASev0: sev0 > 0 ? Math.round((sev0OLA / sev0) * 1000) / 10 : 0,
        aderenciaOLASev1: sev1 > 0 ? Math.round((sev1OLA / sev1) * 1000) / 10 : 0
    };
}

/**
 * Determina o intervalo de datas do "período anterior equivalente" para fins de comparação:
 * - Filtro de Mês específico -> mês anterior (considerando virada de ano)
 * - Filtro de Trimestre específico -> trimestre anterior (considerando virada de ano)
 * - Demais casos (Período Completo, Freeze, T4 Indústria, etc.) -> mesmo intervalo no ano anterior
 */
function computePreviousPeriod(year, periodKey, startDate, endDate) {
    year = parseInt(year, 10);
    const monthNames = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
        'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'];

    if (periodKey && periodKey.indexOf('Mes_') === 0) {
        const m = parseInt(periodKey.split('_')[1], 10);
        let py = year, pm = m - 1;
        if (pm < 0) { pm = 11; py = year - 1; }
        const start = new Date(py, pm, 1, 0, 0, 0);
        const end = new Date(py, pm + 1, 0, 23, 59, 59);
        return { year: py, start: start.getTime(), end: end.getTime(), label: `${monthNames[pm]}/${py}` };
    }

    if (periodKey && periodKey.indexOf('Trimestre_') === 0) {
        const q = parseInt(periodKey.split('_')[1], 10);
        let py = year, pq = q - 1;
        if (pq < 1) { pq = 4; py = year - 1; }
        const startMonth = (pq - 1) * 3;
        const start = new Date(py, startMonth, 1, 0, 0, 0);
        const end = new Date(py, startMonth + 3, 0, 23, 59, 59);
        return { year: py, start: start.getTime(), end: end.getTime(), label: `Q${pq}/${py}` };
    }

    // Demais casos: mesmo intervalo, ano anterior (Período Completo vira YTD espelhado)
    const py = year - 1;
    let start, end;
    if (!startDate && !endDate) {
        const today = new Date();
        const mirrorToday = new Date(today);
        mirrorToday.setFullYear(py);
        start = new Date(py, 0, 1, 0, 0, 0);
        end = mirrorToday;
    } else {
        start = startDate ? new Date(startDate) : new Date(py, 0, 1);
        end = endDate ? new Date(endDate) : new Date();
        start.setFullYear(py);
        end.setFullYear(py);
    }
    return { year: py, start: start.getTime(), end: end.getTime(), label: `${py} (mesmo período)` };
}

/**
 * Busca os KPIs do "período anterior equivalente" para exibir comparativos nos cards executivos.
 */
function getComparisonData(year, periodKey, startDate, endDate) {
    try {
        const prev = computePreviousPeriod(year, periodKey, startDate, endDate);
        if (!prev) return { available: false };
        const kpis = getPeriodKpisOnly(prev.year, prev.start, prev.end);
        if (!kpis) return { available: false };
        return { available: true, label: prev.label, kpis: kpis };
    } catch (e) {
        return { error: e.toString() };
    }
}

/**
 * Busca Dados da Aba Major Problems
 */
function getMajorProblemsData(year) {
  try {
     year = parseInt(year || new Date().getFullYear(), 10);
     const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
     
     let targetSheetName = "MajorProblems";
     if (year == 2026) targetSheetName = "MajorProblems2026";
     
     const sheet = ss.getSheetByName(targetSheetName);

     if (!sheet) return { error: `Aba '${targetSheetName}' não encontrada.` };

     const data = sheet.getDataRange().getValues();
     const rows = data.slice(1); // Skip header

     // New Columns Map (Indices A-H)
     const COL_MP_INCIDENTE = 0;   // A
     const COL_MP_RCA_ID = 1;      // B
     const COL_MP_STATUS = 2;      // C
     const COL_MP_RESP = 3;        // D
     const COL_MP_AREA = 4;        // E
     const COL_MP_DATA_INC = 5;    // F
     const COL_MP_OPEN_DATE = 6;   // G
     const COL_MP_RESOL_DATE = 7;  // H

     const metrics = {
         totalIncidentes: new Set(),
         totalProblemas: new Set(),
         problemasAbertos: 0,
         problemasResolvidos: 0,
         problemasImpl: 0,
         rcaPendentes: 0,
         
         sumResolvedAging: 0,
         countResolvedAging: 0,
         sumMTTRC: 0,
         countMTTRC: 0,
         sumBacklogAging: 0,
         countBacklogAging: 0,

         areaBacklog: {},
         areaResolved: {},

         monthly: {},
         monthlyYTD: {},
         weeklyWTD: {}
     };

     // Robust Date Parser
     const parseDate = (val) => {
         if (val instanceof Date) return val;
         if (!val) return null;
         if (typeof val === 'string' && val.includes("/")) {
             const parts = val.split(/[/\s:]/);
             if (parts.length >= 3) {
                 // Try DD/MM/YYYY or MM/DD/YYYY based on context - usually DD/MM/YYYY in PT-BR
                 const d = parseInt(parts[0], 10);
                 const m = parseInt(parts[1], 10) - 1;
                 const y = parseInt(parts[2], 10);
                 const hh = parseInt(parts[3] || 0, 10);
                 const mm = parseInt(parts[4] || 0, 10);
                 const ss = parseInt(parts[5] || 0, 10);
                 const date = new Date(y, m, d, hh, mm, ss);
                 return isNaN(date.getTime()) ? null : date;
             }
         }
         return null;
     };


     const uniqueProblemData = new Map();
     const today = new Date();

     rows.forEach(row => {
         const incId = String(row[COL_MP_INCIDENTE] || "").trim();
         const prbId = String(row[COL_MP_RCA_ID] || "").trim();
         const status = String(row[COL_MP_STATUS] || "").trim().toLowerCase();
         
         if (!prbId || prbId === "" || prbId === "null") return;

         const dataInc = parseDate(row[COL_MP_DATA_INC]);
         const openDate = parseDate(row[COL_MP_OPEN_DATE]);
         const resolDate = parseDate(row[COL_MP_RESOL_DATE]);

         if (!dataInc || !openDate) return;

         // FILTER BY YEAR (Opening Date)
         if (year && openDate.getFullYear() != year) return;

         // Mes from Data Inc (Col F)
         const mes = dataInc.toLocaleString('pt-BR', { month: 'long' }).toLowerCase();

         if (incId) metrics.totalIncidentes.add(incId);
         metrics.totalProblemas.add(prbId);

         // Aging Calc
         let resAging = 0;
         let backAging = 0;
         if (status === 'resolved' || status === 'closed') {
             if (resolDate) resAging = Math.max(0, Math.floor((resolDate - openDate) / (1000 * 60 * 60 * 24)));
         } else {
             backAging = Math.max(0, Math.floor((today - openDate) / (1000 * 60 * 60 * 24)));
         }

         if (!uniqueProblemData.has(prbId)) {
             uniqueProblemData.set(prbId, {
                 status: status,
                 mes: mes,
                 openDate: openDate,
                 resolDate: resolDate,
                 backlogAging: backAging,
                 resolvedAging: resAging,
                 area: String(row[COL_MP_AREA] || "N/A").trim(),
                 incidents: new Set()
             });
         }
         if (incId) uniqueProblemData.get(prbId).incidents.add(incId);

         // Tab 1 Agg
         if (!metrics.monthly[mes]) {
             metrics.monthly[mes] = { incidents: new Set(), resolvedIds: new Set(), implementationIds: new Set(), rcaPendingIds: new Set() };
         }
         if (incId) metrics.monthly[mes].incidents.add(incId);
         if (status === 'root cause analysis' || status === 'new') metrics.monthly[mes].rcaPendingIds.add(prbId);
         else if (status === 'fix in progress') metrics.monthly[mes].implementationIds.add(prbId);
         else if (status === 'resolved' || status === 'closed') metrics.monthly[mes].resolvedIds.add(prbId);
     });

     // Multi-incident alignment
     uniqueProblemData.forEach((p, prbId) => {
         const status = p.status;
         
         if (status === 'resolved' || status === 'closed') {
             metrics.problemasResolvidos += 1; // Count as 1 problem
             if (p.resolvedAging !== null) {
                 metrics.sumResolvedAging += p.resolvedAging;
                 metrics.countResolvedAging += 1;
             }
             if (p.openDate && p.resolDate) {
                 const week = "Semana " + getWeekNumber(p.resolDate); // TREND BY CLOSING DATE (WTD)
                 if (!metrics.weeklyWTD[week]) metrics.weeklyWTD[week] = { resolved: 0, totalAging: 0, countAging: 0 };
                 metrics.weeklyWTD[week].resolved += 1;
                 metrics.weeklyWTD[week].totalAging += p.resolvedAging;
                 metrics.weeklyWTD[week].countAging += 1;
             }
             let openMonth = p.openDate.toLocaleString('pt-BR', { month: 'long' }).toLowerCase(); // TREND BY OPENING DATE (YTD)
             if (!metrics.monthlyYTD[openMonth]) metrics.monthlyYTD[openMonth] = { resolved: 0, totalAging: 0, countAging: 0 };
             metrics.monthlyYTD[openMonth].resolved += 1;
             metrics.monthlyYTD[openMonth].totalAging += p.resolvedAging;
             metrics.monthlyYTD[openMonth].countAging += 1;
         } else {
             metrics.problemasAbertos += 1; // Count as 1 problem
             if (status === 'fix in progress') metrics.problemasImpl += 1;
             if (status === 'root cause analysis' || status === 'new') {
                 metrics.rcaPendentes += 1;
                 
                 // Backlog Aging represents RCA Pending + New
                 metrics.sumBacklogAging += p.backlogAging;
                 metrics.countBacklogAging += 1;
             }
         }
     });

      // --- MTTRC and RANKING Calculation from RCATask_List Sheet ---
      const taskSheet = ss.getSheetByName("RCATask_List");
      if (taskSheet) {
          const taskData = taskSheet.getDataRange().getValues();
          const taskRows = taskData.slice(1);
          
          const parseDate = (val) => {
              if (val instanceof Date) return val;
              if (typeof val === 'string' && val.includes("/")) {
                  // DD/MM/YYYY HH:MM:SS
                  const parts = val.split(/[/\s:]/);
                  if (parts.length >= 3) {
                      return new Date(parts[2], parts[1] - 1, parts[0], parts[3] || 0, parts[4] || 0, parts[5] || 0);
                  }
              }
              return null;
          };

          const today = new Date();

          const processedTasks = new Set();
          taskRows.forEach(row => {
              const taskNumber = String(row[0] || "").trim(); // Coluna A: Number
              if (!taskNumber || processedTasks.has(taskNumber)) return;
              processedTasks.add(taskNumber);

              const status = String(row[1] || "").trim().toLowerCase(); // Column B: state
              const openedAt = parseDate(row[2]); // Column C: opened_at
              const closedAt = parseDate(row[3]); // Column D: closed_at
              const group = String(row[6] || "N/A").trim(); // Column G: assignment_group
              const taskType = String(row[10] || "").trim().toLowerCase(); // Column K: problem_task_type
              
              if (taskType !== 'root cause analysis') return;

               // Filter by year based on opened_at (or closed_at for resolved)
               if (openedAt && year && openedAt.getFullYear() !== year) return;

              // Logic for MTTRC (Already existing, keeping it)
              if (status === 'closed' && openedAt && closedAt) {
                  const diffHours = (closedAt.getTime() - openedAt.getTime()) / (1000 * 60 * 60);
                  if (diffHours >= 0) {
                      metrics.sumMTTRC += diffHours;
                      metrics.countMTTRC++;
                  }
              }

              // Logic for RANKINGS (Using Columns G, B, K, D, C)
              // Calculation: aging = (closedAt || today) - openedAt
              // User specifically asked for Column D - Column C for both.
              // If not closed, Column D might be null. We'll fallback to today if null to avoid NaN.
              const targetEndDate = (status === 'closed' && closedAt) ? closedAt : today;
              let agingDays = 0;
              if (openedAt) {
                  agingDays = Math.max(0, Math.floor((targetEndDate - openedAt) / (1000 * 60 * 60 * 24)));
              }

                if (status !== 'closed') {
                    // TOP 5 Backlog - Qualquer status que não seja estritamente 'closed' entra no backlog
                    if (!metrics.areaBacklog[group]) metrics.areaBacklog[group] = { count: 0, totalAging: 0 };
                    metrics.areaBacklog[group].count++;
                    metrics.areaBacklog[group].totalAging += agingDays;
                } else {
                    // TOP 5 Resolved - Apenas status 'closed'
                    if (!metrics.areaResolved[group]) metrics.areaResolved[group] = { count: 0, totalAging: 0 };
                    metrics.areaResolved[group].count++;
                    metrics.areaResolved[group].totalAging += agingDays;
                }
          });
      }

      // Final cleanup and averages
      for (let m in metrics.monthly) {
          metrics.monthly[m].incidentsCount = metrics.monthly[m].incidents.size;
          metrics.monthly[m].resolved = metrics.monthly[m].resolvedIds.size;
          metrics.monthly[m].implementation = metrics.monthly[m].implementationIds.size;
          metrics.monthly[m].rcaPending = metrics.monthly[m].rcaPendingIds.size;
          delete metrics.monthly[m].incidents;
          delete metrics.monthly[m].resolvedIds;
          delete metrics.monthly[m].implementationIds;
          delete metrics.monthly[m].rcaPendingIds;
      }

      // Format YTD / WTD
      const formatGroup = (group) => {
          for (let k in group) {
              group[k].avgAging = group[k].countAging > 0 ? Math.round(group[k].totalAging / group[k].countAging) : 0;
              delete group[k].totalAging;
              delete group[k].countAging;
          }
      };
      formatGroup(metrics.monthlyYTD);
      formatGroup(metrics.weeklyWTD);

      const result = {
          kpis: {
              totalIncidentes: metrics.totalIncidentes.size,
              totalProblemas: metrics.totalProblemas.size,
              problemasAbertos: metrics.problemasAbertos,
              problemasResolvidos: metrics.problemasResolvidos,
              problemasImpl: metrics.problemasImpl,
              rcaPendentes: metrics.rcaPendentes,
              avgAgingResolved: metrics.countResolvedAging > 0 ? Math.round(metrics.sumResolvedAging / metrics.countResolvedAging) : 0,
              avgMTTRC: metrics.countMTTRC > 0 ? Math.round(metrics.sumMTTRC / metrics.countMTTRC) : 0,
              avgAgingBacklog: metrics.countBacklogAging > 0 ? Math.round(metrics.sumBacklogAging / metrics.countBacklogAging) : 0,
              mttrcCount: metrics.countMTTRC // Debugging
          },
          topBacklogTeams: Object.entries(metrics.areaBacklog)
            .map(([name, d]) => ({ name, count: d.count, avgAging: Math.round(d.totalAging / d.count) }))
            .sort((a, b) => b.count - a.count)
            .slice(0, 5),
          topResolvedTeams: Object.entries(metrics.areaResolved)
            .map(([name, d]) => ({ name, count: d.count, avgAging: Math.round(d.totalAging / d.count) }))
            .sort((a, b) => b.count - a.count)
            .slice(0, 5),
          monthly: metrics.monthly,
          monthlyYTD: metrics.monthlyYTD,
          weeklyWTD: metrics.weeklyWTD
      };
      
     Logger.log("getMajorProblemsData Result: " + JSON.stringify(result).substring(0, 500));
     return result;

  } catch (e) {
      Logger.log("Erro getMajorProblemsData: " + e.toString());
      return { error: e.message };
  }
}

/**
 * Helpers para Etiquetas de Período
 */
function getWeekLabel(date) {
  return "W" + getWeekNumber(date);
}

function getWeekNumber(d) {
  d = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  var yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  var weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return weekNo;
}

function getQuarterLabel(date) {
  const month = date.getMonth();
  if (month < 3) return "Q1";
  if (month < 6) return "Q2";
  if (month < 9) return "Q3";
  return "Q4";
}