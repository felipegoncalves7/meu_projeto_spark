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
const COL_PROBLEMA = 13;   // N

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
function getFilteredData(year, selectedPeriodKey, startDate, endDate, selectedCard) {
  try {
    year = year || new Date().getFullYear();
    selectedPeriodKey = selectedPeriodKey || 'All';
    selectedCard = selectedCard || 'total';
   
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
           },
           monthlyMetrics: {},
           mttrPorMesEmHoras: {},
           techMetrics: {},
           offenderMetrics: {}
        };
    }

    const durationDisplayValues = sheetDados.getRange(2, COL_DURACAO + 1, dataRows.length, 1).getDisplayValues();
   
    const start = startDate ? new Date(startDate) : null;
    const end = endDate ? new Date(endDate) : null;
    if (end) end.setHours(23, 59, 59, 999);

    let metrics = {
        incidentesTotal: 0, totalDuracaoMinutos: 0,
        sev0Incidentes: 0, sev0DuracaoMinutos: 0,
        sev1Incidentes: 0, sev1DuracaoMinutos: 0,
        monthlyMetrics: {},
        weeklyMetrics: {},
        quarterlyMetrics: {},
        techMetrics: {},
        offenderMetrics: {},
        rawIncidents: []
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

        // KPIs (agora com filtro Tecnologia = SIM applied)
        metrics.incidentesTotal++;
        metrics.totalDuracaoMinutos += durMin;
        
        if (!metrics.monthlyMetrics[mes]) metrics.monthlyMetrics[mes] = { count: 0, durationMin: 0 };
        metrics.monthlyMetrics[mes].count++;
        metrics.monthlyMetrics[mes].durationMin += durMin;

        // Weekly & Quarterly
        if (openDate) {
          const weekLabel = getWeekLabel(openDate);
          if (!metrics.weeklyMetrics[weekLabel]) metrics.weeklyMetrics[weekLabel] = { count: 0, durationMin: 0 };
          metrics.weeklyMetrics[weekLabel].count++;
          metrics.weeklyMetrics[weekLabel].durationMin += durMin;

          const quarterLabel = getQuarterLabel(openDate);
          if (!metrics.quarterlyMetrics[quarterLabel]) metrics.quarterlyMetrics[quarterLabel] = { count: 0, durationMin: 0 };
          metrics.quarterlyMetrics[quarterLabel].count++;
          metrics.quarterlyMetrics[quarterLabel].durationMin += durMin;
        }

        if (isSev0) {
        metrics.sev0Incidentes++;
        metrics.sev0DuracaoMinutos += durMin;
        } else if (isSev1) {
        metrics.sev1Incidentes++;
        metrics.sev1DuracaoMinutos += durMin;
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

    return {
        kpis: {
        incidentesTotal: metrics.incidentesTotal,
        mttrTotal: calculateMTTR(metrics.totalDuracaoMinutos, metrics.incidentesTotal),
        incidentesSev0: metrics.sev0Incidentes,
        mttrSev0: calculateMTTR(metrics.sev0DuracaoMinutos, metrics.sev0Incidentes),
        incidentesSev1: metrics.sev1Incidentes,
        mttrSev1: calculateMTTR(metrics.sev1DuracaoMinutos, metrics.sev1Incidentes),
        },
        monthlyMetrics: metrics.monthlyMetrics,
        mttrPorMesEmHoras: mttrPorMesEmHoras,
        weeklyMetrics: metrics.weeklyMetrics,
        mttrSemanalEmHoras: mttrSemanalEmHoras,
        quarterlyMetrics: metrics.quarterlyMetrics,
        mttrTrimestralEmHoras: mttrTrimestralEmHoras,
        techMetrics: metrics.techMetrics,
        offenderMetrics: metrics.offenderMetrics,
        rawIncidents: metrics.rawIncidents
    };
  } catch (e) {
      return { error: "Erro no servidor: " + e.message };
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