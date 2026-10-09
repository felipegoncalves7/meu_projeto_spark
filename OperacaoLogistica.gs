// ARQUIVO: OperacaoLogistica.gs
// Backend da Visão O&L (Operação & Logística). Isolado do backend da Visão Geral (Código.gs),
// reaproveitando apenas helpers comuns (SPREADSHEET_ID, SHEET_CONFIG, COL_*, buildCallerMap,
// classifyCaller e parseDurationString). Todas as constantes/funções daqui usam o prefixo OL_/ol
// para não colidir com o escopo global compartilhado do Apps Script.
//
// Fontes (ver pasta docs/):
//  - Major_Incidents_Impactos_[Ano]: 1 linha por impacto (Incidente x Mercado x Localidade x Impacto).
//    O&L = Stream "O&L" ou "Planejamento Logístico". A unidade de análise dos cards de Localidade é o
//    par Incidente + Localidade (um mesmo incidente pode ter várias linhas de impacto na mesma localidade).
//  - MajorIncidentes[Ano]: TTR oficial do Incidente (mesma base de MTTR/OLA da Visão Geral).
//  - Disponibilidade_[Ano]: indicador oficial (L0..L5) com Mínimo/Meta/Desafio (colunas U/V/W).
//  - Status_Apuracao (A1:B13): B1 = ano de referência; B2:B13 = Apurado | Parcial | Não iniciado.

const OL_IMPACT_SHEET_PREFIX = 'Major_Incidents_Impactos_';
const OL_DISP_SHEET_PREFIXES = ['Disponibilidade_', 'DISP_'];
const OL_STATUS_SHEET = 'Status_Apuracao';
// Aba opcional com os Incidentes que o negócio classifica como Outlier (A: Incidente, B: Motivo, C: Ano).
const OL_OUTLIERS_SHEET = 'Outliers_OL';
// Marca considerada na apuração (o indicador de Disponibilidade é da Natura; linhas de outras marcas são ignoradas).
const OL_APURACAO_BRAND = 'natura';

// Colunas da aba Major_Incidents_Impactos_[Ano] (base 0) — docs/abas_Major_Incidents_Impactos_[ano]
const OL_COL_INCIDENTE = 0;        // A
const OL_COL_STREAM = 1;           // B
const OL_COL_MERCADO = 2;          // C
const OL_COL_LOCALIDADE = 3;       // D
const OL_COL_IMPACTO = 4;          // E
const OL_COL_SINTOMA = 5;          // F
const OL_COL_APURADO_DISP = 6;     // G - Apurado no Indicador de Disponibilidade ?
const OL_COL_STATUS_APURACAO = 7;  // H - Se sim, qual é o Status
const OL_COL_SEVERIDADE = 8;       // I
const OL_COL_MES = 9;              // J
const OL_COL_ABERTURA = 10;        // K
const OL_COL_ENCERRAMENTO = 11;    // L
const OL_COL_DURACAO = 12;         // M - Duração Impacto (linha)
const OL_COL_DURACAO_CD = 13;      // N - Duração Impacto (CD's): 1ª ocorrência da localidade
const OL_COL_DURACAO_MI = 14;      // O - Duração Impacto (MI): 1ª ocorrência do incidente
const OL_COL_TECNOLOGIA = 15;      // P - Origem Tecnologia (Sim/Não/Outro)
const OL_COL_TITULO = 16;          // Q
const OL_COL_DESC_IMPACTO = 17;    // R
const OL_COL_DESC_OFENSOR = 18;    // S
const OL_COL_SOLUCAO = 19;         // T
const OL_COL_PROBLEM = 20;         // U
const OL_COL_TECH_IMPACTADA = 21;  // V
const OL_COL_OFENSOR = 22;         // W
const OL_TOTAL_COLS = 23;          // A..W (X/Y não são usadas)

// Streams consideradas O&L (comparação sem acento/caixa/espaços)
const OL_STREAMS = { 'o&l': 'O&L', 'planejamentologistico': 'Planejamento Logístico' };

// Fluxos de O&L e palavras-chave da coluna Impacto (E). A ordem define a prioridade da classificação.
// Palavras curtas (<= 4 caracteres, ex.: URA, CRM, GTA, O9) só casam como palavra inteira
// ("URA" não pode casar dentro de "FATURAMENTO").
const OL_FLOWS = [
  { key: 'planejamento', label: 'Fluxo de Planejamento', short: 'Planejamento', icon: 'calendar', entityType: 'sistema',
    keywords: ['sap apo', 'apo', 'o9'] },
  { key: 'manufatura', label: 'Fluxo de Manufatura', short: 'Manufatura', icon: 'factory', entityType: 'localidade',
    keywords: ['fabricacao', 'evolutio', 'gta', 'plantsuite', 'plant suite'] },
  { key: 'atendimento', label: 'Fluxo de Atendimento', short: 'Atendimento', icon: 'headset', entityType: 'sistema',
    keywords: ['crm', 'ura'] },
  { key: 'separacao', label: 'Fluxo de Separação, Faturamento e Transporte', short: 'Separação, Faturamento e Transporte', icon: 'box', entityType: 'localidade',
    keywords: ['separacao', 'faturamento', 'expedicao', 'facturacion', 'separacion'] }
];
// Ordem de exibição na tela (diferente da ordem de prioridade da classificação acima)
const OL_FLOW_DISPLAY_ORDER = ['separacao', 'manufatura', 'atendimento', 'planejamento'];

// Catálogo de Localidades / Sistemas por fluxo (exibidas mesmo sem incidentes no período).
// kind: cd | hub | pea | planta | ecoparque | sistema. aliases: nomes alternativos (sem acento/caixa).
const OL_CATALOGO = {
  separacao: [
    { name: 'CD Cabreúva', country: 'br', kind: 'cd' },
    { name: 'CD Cajamar', country: 'br', kind: 'cd' },
    { name: 'CD Castanhal', country: 'br', kind: 'cd' },
    { name: 'CD Matias Barbosa', country: 'br', kind: 'cd' },
    { name: 'CD Murici', country: 'br', kind: 'cd' },
    { name: 'CD São Paulo', country: 'br', kind: 'cd' },
    { name: 'CD Simões Filho', country: 'br', kind: 'cd' },
    { name: 'CD Uberlândia', country: 'br', kind: 'cd' },
    { name: 'HUB Cabreúva', country: 'br', kind: 'hub' },
    { name: 'HUB Itupeva', country: 'br', kind: 'hub' },
    { name: 'PEA Cachoeirinha', country: 'br', kind: 'pea' },
    { name: 'PEA Manaus', country: 'br', kind: 'pea' },
    { name: 'CD Pudahuel', country: 'cl', kind: 'cd' },
    { name: 'CD Guarne', country: 'co', kind: 'cd' },
    { name: 'CD Amaguaña', country: 'ec', kind: 'cd' },
    { name: 'CD Celaya', country: 'mx', kind: 'cd' },
    { name: 'CD Lurín', country: 'pe', kind: 'cd' }
  ],
  manufatura: [
    { name: 'Ecoparque', country: 'br', kind: 'ecoparque' },
    { name: 'Rio Amazonas (Planta Cajamar)', country: 'br', kind: 'planta', aliases: ['rio amazonas'] },
    { name: 'São Francisco (Planta Cajamar)', country: 'br', kind: 'planta', aliases: ['sao francisco'] },
    { name: 'Rio da Prata (Planta Cajamar)', country: 'br', kind: 'planta', aliases: ['rio da prata'] },
    { name: 'Interlagos', country: 'br', kind: 'planta', inactiveNote: 'Inativada em 2025', inactiveAfterYear: 2025 },
    { name: 'Moreno', country: 'ar', kind: 'planta' },
    { name: 'Celaya', country: 'mx', kind: 'planta', aliases: ['planta celaya'] }
  ],
  atendimento: [
    { name: 'CRM', kind: 'sistema', keywords: ['crm'] },
    { name: 'URA', kind: 'sistema', keywords: ['ura'] }
  ],
  planejamento: [
    { name: 'SAP APO', kind: 'sistema', keywords: ['sap apo', 'apo'], years: [2025] },
    { name: 'O9', kind: 'sistema', keywords: ['o9'], years: [2026] }
  ]
};

// Mercado (coluna C) -> [ISO alpha-2, Nome]. Aceita sigla (BR, EQ...) ou nome do país.
const OL_MERCADOS = {
  br: ['br', 'Brasil'], brasil: ['br', 'Brasil'],
  ar: ['ar', 'Argentina'], argentina: ['ar', 'Argentina'],
  cl: ['cl', 'Chile'], chile: ['cl', 'Chile'],
  co: ['co', 'Colômbia'], colombia: ['co', 'Colômbia'],
  pe: ['pe', 'Peru'], peru: ['pe', 'Peru'],
  mx: ['mx', 'México'], mexico: ['mx', 'México'],
  eq: ['ec', 'Equador'], ec: ['ec', 'Equador'], equador: ['ec', 'Equador'], ecuador: ['ec', 'Equador'],
  gt: ['gt', 'Guatemala'], guatemala: ['gt', 'Guatemala'],
  do: ['do', 'República Dominicana'], 'republica dominicana': ['do', 'República Dominicana'],
  hn: ['hn', 'Honduras'], honduras: ['hn', 'Honduras'],
  ni: ['ni', 'Nicarágua'], nicaragua: ['ni', 'Nicarágua'],
  pa: ['pa', 'Panamá'], panama: ['pa', 'Panamá'],
  sv: ['sv', 'El Salvador'], 'el salvador': ['sv', 'El Salvador'],
  cr: ['cr', 'Costa Rica'], 'costa rica': ['cr', 'Costa Rica'],
  uy: ['uy', 'Uruguai'], uruguai: ['uy', 'Uruguai'], uruguay: ['uy', 'Uruguai'],
  my: ['my', 'Malásia'], malasia: ['my', 'Malásia']
};

const OL_MONTHS_PT = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'];

function olIsDate(v) { return Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v.getTime()); }

/** Remove acentos, caixa e espaços duplicados. */
function olFold(v) {
  return String(v === null || v === undefined ? '' : v)
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase().replace(/\s+/g, ' ').trim();
}

/** Casa uma palavra-chave no texto (já normalizado): palavra inteira para termos curtos, substring para os demais. */
function olMatchKeyword(text, kw) {
  if (!text || !kw) return false;
  if (kw.length <= 4) {
    const escaped = kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp('(^|[^a-z0-9])' + escaped + '([^a-z0-9]|$)').test(text);
  }
  return text.indexOf(kw) !== -1;
}

function olMercado(raw) {
  const k = olFold(raw);
  if (!k) return { iso: '', name: '' };
  const hit = OL_MERCADOS[k];
  return hit ? { iso: hit[0], name: hit[1] } : { iso: k.length === 2 ? k : '', name: String(raw).trim() };
}

/** Duração em minutos a partir do texto exibido: ISO-8601 (PT02H54M00.000S), HH:MM, HH:MM:SS ou DD:HH:MM:SS. */
function olParseDuration(display) {
  const s = String(display === null || display === undefined ? '' : display).trim();
  if (!s || s === '-' || s.charAt(0) === '#') return null;
  const iso = s.match(/^P(?:(\d+(?:[.,]\d+)?)D)?(?:T(?:(\d+(?:[.,]\d+)?)H)?(?:(\d+(?:[.,]\d+)?)M)?(?:(\d+(?:[.,]\d+)?)S)?)?$/i);
  if (iso && s.length > 1) {
    const n = x => x ? parseFloat(String(x).replace(',', '.')) : 0;
    return Math.round(n(iso[1]) * 1440 + n(iso[2]) * 60 + n(iso[3]) + n(iso[4]) / 60);
  }
  if (s.indexOf(':') !== -1) return parseDurationString(s);
  const num = parseFloat(s.replace(',', '.'));
  return isFinite(num) ? Math.round(num) : null;
}

function olParseSeveridade(v) {
  const m = String(v === null || v === undefined ? '' : v).match(/\d/);
  return m ? Number(m[0]) : null;
}

function olParseMonth(rawMes, abertura) {
  if (olIsDate(rawMes)) return rawMes.getMonth() + 1;
  const k = olFold(rawMes);
  if (k) {
    const idx = OL_MONTHS_PT.findIndex(m => k === m || k.indexOf(m) === 0 || (k.length >= 3 && m.indexOf(k) === 0));
    if (idx !== -1) return idx + 1;
    const n = parseInt(k, 10);
    if (n >= 1 && n <= 12) return n;
  }
  return olIsDate(abertura) ? abertura.getMonth() + 1 : null;
}

function olClassifyFlow(impactoFold, streamKey) {
  for (let i = 0; i < OL_FLOWS.length; i++) {
    const f = OL_FLOWS[i];
    if (f.keywords.some(kw => olMatchKeyword(impactoFold, kw))) return f.key;
  }
  return streamKey === 'planejamentologistico' ? 'planejamento' : 'outros';
}

/** Identifica a Localidade/Sistema (entidade do card) de uma linha de impacto dentro do seu fluxo. */
function olResolveEntity(flowKey, localidadeRaw, impactoFold, mercado) {
  const catalog = OL_CATALOGO[flowKey] || [];
  if (flowKey === 'atendimento' || flowKey === 'planejamento') {
    const sys = catalog.find(c => (c.keywords || []).some(kw => olMatchKeyword(impactoFold, kw)));
    if (sys) return { key: 'sys:' + olFold(sys.name), name: sys.name, kind: 'sistema', country: '' };
    return { key: 'sys:outros', name: 'Outros sistemas', kind: 'sistema', country: '' };
  }

  const loc = olFold(localidadeRaw);
  if (loc) {
    const names = catalog.map(c => ({ c: c, n: olFold(c.name), a: (c.aliases || []).map(olFold) }));
    let hit = names.find(x => x.n === loc || x.a.indexOf(loc) !== -1);
    if (!hit) hit = names.find(x => loc.indexOf(x.n) !== -1 || x.a.some(a => loc.indexOf(a) !== -1));
    if (!hit) {
      const partial = names.filter(x => x.n.indexOf(loc) !== -1);
      if (partial.length === 1) hit = partial[0];
    }
    if (hit) return { key: 'loc:' + hit.n, name: hit.c.name, kind: hit.c.kind, country: hit.c.country || mercado.iso };
    const kind = /^cd\b/.test(loc) ? 'cd' : /^hub\b/.test(loc) ? 'hub' : /^pea\b/.test(loc) ? 'pea'
      : (/planta|fabrica/.test(loc) ? 'planta' : (flowKey === 'manufatura' ? 'planta' : 'cd'));
    return { key: 'loc:' + loc, name: String(localidadeRaw).trim(), kind: kind, country: mercado.iso };
  }
  // Sem Localidade informada (ex.: América Central e Uruguai, onde não há o nome exato do CD): agrupa por Mercado
  const iso = mercado.iso || 'nd';
  return {
    key: 'mkt:' + iso,
    name: mercado.name ? 'Operação ' + mercado.name : 'Localidade não informada',
    kind: flowKey === 'manufatura' ? 'planta' : 'cd',
    country: mercado.iso,
    unnamed: true
  };
}

/**
 * Lê a aba Status_Apuracao. Vale para o ano de referência (B1); anos anteriores são considerados
 * apurados e anos posteriores, não iniciados.
 */
function olReadStatusApuracao(ss) {
  const res = { found: false, refYear: null, months: null, warnings: [] };
  try {
    const sheet = ss.getSheetByName(OL_STATUS_SHEET);
    if (!sheet) { res.warnings.push('Aba "' + OL_STATUS_SHEET + '" não encontrada: status de apuração inferido pela data atual.'); return res; }
    const v = sheet.getRange(1, 1, 13, 2).getValues();
    let y = v[0][1];
    if (olIsDate(y)) y = y.getFullYear();
    y = Number(y);
    if (!(y >= 2000 && y <= 2100)) { res.warnings.push('Ano de referência inválido em ' + OL_STATUS_SHEET + '!B1.'); return res; }
    const months = [];
    for (let m = 1; m <= 12; m++) {
      const s = olFold(v[m][1]);
      let st;
      if (s === 'apurado' || s === 'fechado' || s === 'concluido') st = 'APURADO';
      else if (s === 'parcial' || s === 'parcialmente apurado' || s === 'em apuracao' || s === 'em andamento') st = 'PARCIAL';
      else {
        if (s && s !== 'nao iniciado' && s !== 'nao iniciada') res.warnings.push(OL_STATUS_SHEET + '!B' + (m + 1) + ': status desconhecido "' + v[m][1] + '" (tratado como Não iniciado).');
        st = 'NAO_INICIADO';
      }
      months.push(st);
    }
    res.found = true; res.refYear = y; res.months = months;
  } catch (e) {
    res.warnings.push('Falha ao ler ' + OL_STATUS_SHEET + ': ' + e.message);
  }
  return res;
}

function olStatusForYear(statusInfo, year) {
  const months = [];
  if (statusInfo.found) {
    for (let m = 1; m <= 12; m++) {
      if (year < statusInfo.refYear) months.push('APURADO');
      else if (year > statusInfo.refYear) months.push('NAO_INICIADO');
      else months.push(statusInfo.months[m - 1]);
    }
    return months;
  }
  const now = new Date();
  const cy = now.getFullYear(), cm = now.getMonth() + 1;
  for (let m = 1; m <= 12; m++) {
    if (year < cy || (year === cy && m < cm)) months.push('APURADO');
    else if (year === cy && m === cm) months.push('PARCIAL');
    else months.push('NAO_INICIADO');
  }
  return months;
}

/** Mapa Incidente -> TTR oficial / Severidade / Tecnologia, a partir de MajorIncidentes[Ano] (mesma base da Visão Geral). */
function olBuildMajorIncidentMap(ss, year) {
  const map = {};
  const sheet = ss.getSheetByName('MajorIncidentes' + year);
  if (!sheet) return map;
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return map;
  const n = lastRow - 1;
  const values = sheet.getRange(2, 1, n, COL_TECNOLOGIA + 1).getValues();
  const durDisplay = sheet.getRange(2, COL_DURACAO + 1, n, 1).getDisplayValues();
  for (let i = 0; i < n; i++) {
    const id = String(values[i][0] || '').trim();
    if (!id) continue;
    const ttrTxt = String(durDisplay[i][0] || '').trim();
    map[id] = {
      ttr: ttrTxt ? parseDurationString(ttrTxt) : null,
      ttrTxt: ttrTxt,
      sev: olParseSeveridade(values[i][COL_SEVERIDADE]),
      tec: String(values[i][COL_TECNOLOGIA] || '').trim().toUpperCase() === 'SIM'
    };
  }
  return map;
}

/** Lê as linhas de O&L (Stream O&L / Planejamento Logístico) da aba Major_Incidents_Impactos_[Ano]. */
function olReadImpactos(ss, year, callerMap) {
  const sheet = ss.getSheetByName(OL_IMPACT_SHEET_PREFIX + year);
  if (!sheet) return { found: false, rows: [], incidents: {}, unmatchedImpacts: [] };
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return { found: true, rows: [], incidents: {}, unmatchedImpacts: [] };
  const n = lastRow - 1;
  const width = Math.min(OL_TOTAL_COLS, sheet.getLastColumn());
  const values = sheet.getRange(2, 1, n, width).getValues();
  const durDisplay = sheet.getRange(2, OL_COL_DURACAO + 1, n, 3).getDisplayValues(); // M:O

  const miMap = olBuildMajorIncidentMap(ss, year);
  const rows = [];
  const incidents = {};
  const unmatched = {};
  const txt = (v, max) => {
    const s = String(v === null || v === undefined ? '' : v).trim();
    return max && s.length > max ? s.slice(0, max) + '…' : s;
  };

  for (let i = 0; i < n; i++) {
    const r = values[i];
    const inc = txt(r[OL_COL_INCIDENTE]);
    if (!inc) continue;
    const streamKey = olFold(r[OL_COL_STREAM]).replace(/\s+/g, '');
    if (!OL_STREAMS[streamKey]) continue;

    const abertura = olIsDate(r[OL_COL_ABERTURA]) ? r[OL_COL_ABERTURA] : null;
    const encerramento = olIsDate(r[OL_COL_ENCERRAMENTO]) ? r[OL_COL_ENCERRAMENTO] : null;
    const impacto = txt(r[OL_COL_IMPACTO]);
    const impactoFold = olFold(impacto);
    const mercado = olMercado(r[OL_COL_MERCADO]);
    const flow = olClassifyFlow(impactoFold, streamKey);
    const ent = flow === 'outros'
      ? { key: 'outros', name: impacto || 'Impacto não informado', kind: 'cd', country: mercado.iso }
      : olResolveEntity(flow, r[OL_COL_LOCALIDADE], impactoFold, mercado);
    if (flow === 'outros') unmatched[impacto || '(vazio)'] = (unmatched[impacto || '(vazio)'] || 0) + 1;

    let dur = olParseDuration(durDisplay[i][0]);
    if (dur === null && abertura && encerramento && encerramento > abertura) dur = Math.round((encerramento - abertura) / 60000);
    const mi = miMap[inc];
    const tecRaw = olFold(r[OL_COL_TECNOLOGIA]);
    const tec = tecRaw ? tecRaw === 'sim' : (mi ? mi.tec : false);
    const sev = olParseSeveridade(r[OL_COL_SEVERIDADE]);

    rows.push({
      inc: inc,
      st: OL_STREAMS[streamKey],
      mk: mercado.iso,
      mkn: mercado.name,
      loc: txt(r[OL_COL_LOCALIDADE]),
      imp: impacto,
      sin: txt(r[OL_COL_SINTOMA]),
      apd: txt(r[OL_COL_APURADO_DISP]).toUpperCase(),
      aps: txt(r[OL_COL_STATUS_APURACAO]),
      sev: sev,
      m: olParseMonth(r[OL_COL_MES], abertura),
      ab: abertura ? abertura.getTime() : null,
      enc: encerramento ? encerramento.getTime() : null,
      dur: dur,
      durCd: olParseDuration(durDisplay[i][1]),
      durMi: olParseDuration(durDisplay[i][2]),
      tec: tec,
      tecTxt: txt(r[OL_COL_TECNOLOGIA]),
      tit: txt(r[OL_COL_TITULO], 200),
      dImp: txt(r[OL_COL_DESC_IMPACTO], 600),
      dOf: txt(r[OL_COL_DESC_OFENSOR], 600),
      sol: txt(r[OL_COL_SOLUCAO], 400),
      prb: txt(r[OL_COL_PROBLEM]),
      tImp: txt(r[OL_COL_TECH_IMPACTADA]) || 'N/A',
      ofe: txt(r[OL_COL_OFENSOR]) || 'N/A',
      flow: flow,
      ent: ent.key,
      entName: ent.name,
      entKind: ent.kind,
      entCc: ent.country || '',
      entUnnamed: !!ent.unnamed
    });

    if (!incidents[inc]) {
      incidents[inc] = {
        ttr: mi && mi.ttr !== null ? mi.ttr : null,
        ttrTxt: mi ? mi.ttrTxt : '',
        sev: mi && mi.sev !== null ? mi.sev : sev,
        foundMI: !!mi,
        origem: (callerMap && classifyCaller(callerMap[inc])) || 'N/A'
      };
    }
  }

  return {
    found: true,
    rows: rows,
    incidents: incidents,
    unmatchedImpacts: Object.keys(unmatched).map(k => ({ impacto: k, linhas: unmatched[k] })).sort((a, b) => b.linhas - a.linhas)
  };
}

/** Converte um percentual da planilha para a escala 0..100 (fração 0..1 do Sheets ou texto "99,79%"). */
function olPct(v) {
  if (v === null || v === undefined || v === '') return null;
  if (typeof v === 'number') {
    if (!isFinite(v)) return null;
    if (v >= 0 && v <= 1.5) return v * 100;
    if (v > 1.5 && v <= 100) return v;
    return null;
  }
  const s = String(v).trim();
  if (!s || s.charAt(0) === '#' || s === '-' || s === '—') return null;
  const hasPct = s.indexOf('%') !== -1;
  const num = parseFloat(s.replace('%', '').replace(/\./g, '').replace(',', '.'));
  if (!isFinite(num)) return null;
  if (hasPct) return num;
  return num <= 1.5 ? num * 100 : (num <= 100 ? num : null);
}

/**
 * Lê a aba Disponibilidade_[Ano] e devolve a subárvore de O&L (O&L -> Fluxos -> L3 -> L4 -> L5),
 * com os 12 meses, Q1..Q4, YTD, Ano e as referências Mínimo/Meta/Desafio de cada nó.
 */
function olReadDisponibilidade(ss, year) {
  let sheet = null;
  for (let i = 0; i < OL_DISP_SHEET_PREFIXES.length && !sheet; i++) sheet = ss.getSheetByName(OL_DISP_SHEET_PREFIXES[i] + year);
  if (!sheet) return null;
  const lastRow = sheet.getLastRow();
  if (lastRow < 5) return null;
  const values = sheet.getRange(1, 1, lastRow, 23).getValues();

  // Layout C..T: Jan Fev Mar Q1 Abr Mai Jun Q2 Jul Ago Set Q3 Out Nov Dez Q4 YTD Ano
  const MONTH_COLS = [2, 3, 4, 6, 7, 8, 10, 11, 12, 14, 15, 16];
  const QUARTER_COLS = [5, 9, 13, 17];

  const nodes = [];
  const stack = [];
  const root = { level: -1, children: [] };
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    const lv = String(row[0] || '').trim().toUpperCase();
    if (!/^L[0-5]$/.test(lv)) continue;
    const name = String(row[1] || '').trim();
    if (!name) continue;
    const node = {
      name: name,
      level: Number(lv.charAt(1)),
      months: MONTH_COLS.map(c => olPct(row[c])),
      quarters: QUARTER_COLS.map(c => olPct(row[c])),
      ytd: olPct(row[18]),
      fullYear: olPct(row[19]),
      min: olPct(row[20]),
      meta: olPct(row[21]),
      des: olPct(row[22]),
      children: []
    };
    while (stack.length && stack[stack.length - 1].level >= node.level) stack.pop();
    (stack.length ? stack[stack.length - 1] : root).children.push(node);
    stack.push(node);
    nodes.push(node);
  }

  const isOL = n => /^(jornada\s+)?o\s*&\s*l\b/.test(olFold(n.name)) || olFold(n.name) === 'operacao e logistica' || olFold(n.name) === 'operacao & logistica';
  const olNode = nodes.find(isOL);
  if (!olNode) return { found: false, sheet: sheet.getName() };

  const flowKeyOf = olFlowKeyFromName;

  // Fluxos = filhos do nó O&L; se O&L não tiver filhos (fluxos como irmãos no mesmo nível), usa os irmãos seguintes.
  let flowNodes = olNode.children.slice();
  if (!flowNodes.length) {
    const parent = (function findParent(list, target) {
      for (let i = 0; i < list.length; i++) {
        if (list[i].children.indexOf(target) !== -1) return list[i];
        const p = findParent(list[i].children, target);
        if (p) return p;
      }
      return null;
    })([root], olNode);
    const siblings = parent ? parent.children : [];
    const start = siblings.indexOf(olNode);
    for (let i = start + 1; i < siblings.length; i++) {
      if (!flowKeyOf(siblings[i].name)) break;
      flowNodes.push(siblings[i]);
    }
  }

  const strip = n => ({
    name: n.name, level: n.level, months: n.months, quarters: n.quarters, ytd: n.ytd, fullYear: n.fullYear,
    min: n.min, meta: n.meta, des: n.des, children: n.children.map(strip)
  });
  const flows = {};
  flowNodes.forEach(fn => {
    const key = flowKeyOf(fn.name);
    if (key && !flows[key]) flows[key] = Object.assign(strip(fn), { flowKey: key });
  });
  const olOut = strip(olNode);
  olOut.children = [];
  return { found: true, sheet: sheet.getName(), ol: olOut, flows: flows };
}

/** Nome de um Fluxo (aba de Disponibilidade ou de apuração) -> chave do fluxo de O&L. */
function olFlowKeyFromName(name) {
  const k = olFold(name);
  if (/separa|faturamento|transporte|expedi/.test(k)) return 'separacao';
  if (/fabrica|manufatura/.test(k)) return 'manufatura';
  if (/atendimento/.test(k)) return 'atendimento';
  if (/planejamento/.test(k)) return 'planejamento';
  return null;
}

/**
 * Lê a aba apuracao_[Ano] (docs/abas_apuracao_[ano]): minutos de indisponibilidade apurados por
 * Incidente x Fluxo x País x Serviço x Mês. É a base para isolar o efeito de cada incidente na
 * Disponibilidade (visão "sem outliers"). Mantém só Stream O&L / Planejamento Logístico da marca Natura.
 */
function olReadApuracao(ss, year) {
  const out = { found: false, sheet: null, rows: [], stats: { rows: 0, otherBrand: 0, otherStream: 0, noMonth: 0, badMinutes: 0 } };
  const target = 'apuracao_' + year;
  let sheet = null;
  ss.getSheets().forEach(sh => { if (!sheet && olFold(sh.getName()) === target) sheet = sh; });
  if (!sheet) ss.getSheets().forEach(sh => { const f = olFold(sh.getName()); if (!sheet && f.indexOf('apura') !== -1 && f.indexOf(String(year)) !== -1 && f.indexOf('status') === -1) sheet = sh; });
  if (!sheet) return out;
  out.found = true; out.sheet = sheet.getName();
  const lastRow = sheet.getLastRow(), lastCol = sheet.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return out;
  const rng = sheet.getRange(1, 1, lastRow, lastCol);
  const values = rng.getValues(), shown = rng.getDisplayValues();

  let header = -1;
  for (let r = 0; r < Math.min(values.length, 15); r++) if (olFold(values[r][0]) === 'incidente') { header = r; break; }
  const byLabel = {};
  if (header !== -1) values[header].forEach((h, i) => { const k = olFold(h); if (k && !(k in byLabel)) byLabel[k] = i; });
  const col = (label, idx) => (label in byLabel ? byLabel[label] : idx);
  const C = { inc: col('incidente', 0), data: col('data de abertura', 1), marca: col('marca', 2), stream: col('stream', 3), fluxo: col('fluxo', 4), pais: col('pais', 5), serv: col('servico', 6), tempo: col('tempo', 7), mes: col('mes', 9) };

  for (let i = header + 1; i < values.length; i++) {
    const v = values[i], d = shown[i];
    const inc = String(v[C.inc] === null || v[C.inc] === undefined ? '' : v[C.inc]).trim();
    if (!inc) continue;
    out.stats.rows++;
    const marca = olFold(d[C.marca]);
    if (marca && marca !== OL_APURACAO_BRAND) { out.stats.otherBrand++; continue; }
    const streamKey = olFold(d[C.stream]).replace(/\s+/g, '');
    const flow = olFlowKeyFromName(d[C.fluxo]);
    if (!OL_STREAMS[streamKey] && !(streamKey === '' && flow)) { out.stats.otherStream++; continue; }
    let m = parseInt(String(d[C.mes]).trim(), 10);
    if (!(m >= 1 && m <= 12)) m = olIsDate(v[C.data]) ? v[C.data].getMonth() + 1 : null;
    if (!m) { out.stats.noMonth++; continue; }
    const min = typeof v[C.tempo] === 'number' ? v[C.tempo] : parseFloat(String(d[C.tempo]).replace(',', '.'));
    if (!isFinite(min) || min < 0) { out.stats.badMinutes++; continue; }
    out.rows.push({
      inc: inc,
      isInc: /^INC\d+$/i.test(inc),
      m: m,
      flow: flow || (streamKey === 'planejamentologistico' ? 'planejamento' : 'outros'),
      fluxo: String(d[C.fluxo] || '').trim(),
      pais: String(d[C.pais] || '').trim(),
      serv: String(d[C.serv] || '').trim(),
      min: Math.round(min * 100) / 100
    });
  }
  return out;
}

/** Lê a aba Outliers_OL (opcional): Incidentes classificados pelo negócio como Outlier. */
function olReadOutliers(ss) {
  const res = { found: false, items: [] };
  let sheet = ss.getSheetByName(OL_OUTLIERS_SHEET);
  if (!sheet) ss.getSheets().forEach(sh => { const f = olFold(sh.getName()).replace(/[\s_&-]/g, ''); if (!sheet && (f === 'outliersol' || f === 'outliers')) sheet = sh; });
  if (!sheet) return res;
  res.found = true; res.sheet = sheet.getName();
  const lastRow = sheet.getLastRow();
  if (lastRow < 1) return res;
  const values = sheet.getRange(1, 1, lastRow, Math.max(1, Math.min(3, sheet.getLastColumn()))).getValues();
  values.forEach(r => {
    const inc = String(r[0] || '').trim();
    if (!/^INC\d+$/i.test(inc)) return; // ignora cabeçalho e linhas vazias
    res.items.push({ inc: inc.toUpperCase(), motivo: String(r[1] || '').trim(), ano: Number(r[2]) || null });
  });
  return res;
}

function olListYears(ss) {
  const years = {};
  ss.getSheets().forEach(s => {
    const m = s.getName().match(/^Major_Incidents_Impactos_(\d{4})$/i);
    if (m) years[m[1]] = true;
  });
  return Object.keys(years).map(Number).sort((a, b) => b - a);
}

/**
 * Ponto de entrada da Visão O&L: devolve, para o ano selecionado e o ano anterior (comparativos
 * "mesmo período do ano anterior"), as linhas de impacto de O&L, o mapa de Incidentes (TTR oficial,
 * Severidade, Origem da Detecção), a Disponibilidade oficial de O&L e o status de apuração dos meses.
 * Datas são enviadas em milissegundos (google.script.run não serializa Date).
 */
function getOperacaoLogisticaData(year) {
  try {
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const years = olListYears(ss);
    const statusInfo = olReadStatusApuracao(ss);
    const fallbackYear = statusInfo.found ? statusInfo.refYear : new Date().getFullYear();
    year = Number(year) || (years.indexOf(fallbackYear) !== -1 ? fallbackYear : (years[0] || fallbackYear));
    const callerMap = buildCallerMap(ss);

    let lastUpdated = 'N/A';
    const sheetConfig = ss.getSheetByName(SHEET_CONFIG);
    if (sheetConfig) {
      const val = sheetConfig.getRange('B4').getValue();
      if (olIsDate(val)) lastUpdated = val.toLocaleString('pt-BR');
    }

    const buildYear = y => {
      const imp = olReadImpactos(ss, y, callerMap);
      return {
        year: y,
        statuses: olStatusForYear(statusInfo, y),
        impactsFound: imp.found,
        rows: imp.rows,
        incidents: imp.incidents,
        unmatchedImpacts: imp.unmatchedImpacts,
        disponibilidade: olReadDisponibilidade(ss, y),
        apuracao: olReadApuracao(ss, y)
      };
    };

    return {
      ok: true,
      year: year,
      prevYear: year - 1,
      availableYears: years,
      lastUpdated: lastUpdated,
      statusSource: statusInfo.found ? 'SHEET' : 'FALLBACK',
      warnings: statusInfo.warnings,
      current: buildYear(year),
      previous: buildYear(year - 1),
      outliers: olReadOutliers(ss),
      config: {
        flows: OL_FLOW_DISPLAY_ORDER.map(k => {
          const f = OL_FLOWS.find(x => x.key === k);
          return { key: f.key, label: f.label, short: f.short, icon: f.icon, entityType: f.entityType };
        }),
        catalog: OL_CATALOGO,
        targets: {
          olaSev0: OLA_TARGET_SEV0_MIN, olaSev1: OLA_TARGET_SEV1_MIN,
          mttrSev0: MTTR_TARGET_SEV0_MIN, mttrSev1: MTTR_TARGET_SEV1_MIN,
          mttrSemOutliersSev0: MTTR_TARGET_SEM_OUTLIERS_SEV0_MIN, mttrSemOutliersSev1: MTTR_TARGET_SEM_OUTLIERS_SEV1_MIN,
          outlierSev0: MTTR_OUTLIER_SEV0_MIN, outlierSev1: MTTR_OUTLIER_SEV1_MIN
        }
      }
    };
  } catch (e) {
    return { ok: false, error: e.toString() };
  }
}
