// ARQUIVO: OperacaoLogisticaAnalise.gs
// Backend da sub-visão "O&L · Análise Tático-Estratégica" (rota ?view=ol-analise).
// Reaproveita os leitores da Visão O&L (OperacaoLogistica.gs: olReadImpactos, olReadDisponibilidade,
// olReadApuracao, olReadStatusApuracao...) e os helpers da Visão Geral (Código.gs: buildCallerMap,
// classifyCaller, normalizeChangeNumber, metas de MTTR/OLA). Constantes e funções daqui usam o prefixo
// OLX_/olx para não colidir no escopo global do Apps Script.
//
// Unidade de análise: o INCIDENTE de O&L (Stream O&L ou Planejamento Logístico na aba
// Major_Incidents_Impactos_[Ano]) com origem Tecnologia, enriquecido com:
//  - MajorIncidentes[Ano]: TTR oficial, Severidade, Tecnologia Impactada, Ofensor, Problem, Type SM, SM Number;
//  - Major_ServiceNow: Caller (Origem da Detecção), Priority, Grupo, Service, Category, Change Request;
//  - Camada (Aplicação x Infraestrutura): aba opcional OL_Camada + regras por palavra-chave (olxClassifyLayer);
//  - Problemas: MajorProblems / MajorProblems2026, Problem_ServiceNow, PTASK_RCA (Causa do Problema e
//    Processo de Origem classificados na tarefa de RCA) e PTASK_IMP, com estado, datas e equipe das tarefas
//    pela RCATask_List (mesma base do Engajamento por Equipe da Visão Geral);
//  - Mudanças: Change_MI (Type, Service, Grupo, janela planejada).

const OLX_LAYER_SHEET = 'OL_Camada';
// Meta de MTTR da sub-visão: a mesma com ou sem outliers (Sev 0 <= 02:00 e Sev 1 <= 06:00).
const OLX_MTTR_TARGET_SEV0_MIN = 120;
const OLX_MTTR_TARGET_SEV1_MIN = 360;

// Palavras-chave (sem acento/caixa) que indicam Infraestrutura. Termos de até 4 letras casam só como
// palavra inteira (olMatchKeyword), para "rf" não casar dentro de "performance", por exemplo.
const OLX_INFRA_KEYWORDS = [
  'infra', 'infraestrutura', 'rede', 'redes', 'network', 'conectividade', 'connectivity', 'link', 'links', 'internet',
  'wan', 'lan', 'mpls', 'sdwan', 'sd-wan', 'vpn', 'wifi', 'wi-fi', 'wireless', 'access point', 'firewall', 'proxy',
  'dns', 'dhcp', 'switch', 'roteador', 'router', 'operadora', 'telecom', 'telefonia', 'energia', 'eletrica', 'eletrico',
  'nobreak', 'no-break', 'gerador', 'ups', 'datacenter', 'data center', 'servidor', 'servidores', 'server', 'hardware',
  'storage', 'disco', 'vm', 'vmware', 'virtualizacao', 'cluster', 'cloud', 'nuvem', 'gcp', 'aws', 'azure', 'oci',
  'radius', 'active directory', 'ldap', 'certificado', 'citrix', 'sistema operacional', 'backup', 'basis',
  'banco de dados', 'database', 'dba', 'coletor', 'coletores', 'impressora', 'impressoras', 'printer', 'rf',
  'memoria', 'cpu', 'capacidade', 'kubernetes', 'k8s', 'gke', 'container', 'noc', 'ar condicionado', 'refrigeracao',
  // datacenter, rede e segurança de perímetro
  'dc', 'equinix', 'ascenty', 'colocation', 'rack', 'fex', 'nexus', 'fabric extender', 'vlan', 'bgp', 'roteamento', 'fibra',
  'cabeamento', 'embratel', 'load balancer', 'balanceador', 'f5', 'fortinet', 'fortigate', 'palo alto', 'hypervisor',
  'hipervisor', 'esxi', 'vcenter', 'firmware', 'subestacao', 'eletricidade', 'climatizacao'
];
// Termos inequívocos de datacenter/rede: valem também no Título e nas Descrições, antes das palavras de Aplicação
// (ex.: Ofensor "Interface FEX" ou "DC Equinix" é Infraestrutura, mesmo com "interface" na lista de Aplicação).
const OLX_INFRA_STRONG = ['equinix', 'ascenty', 'colocation', 'fex', 'nexus', 'fabric extender'];
// Palavras-chave que indicam Aplicação (decidem quando o Ofensor é claramente de aplicação).
const OLX_APP_KEYWORDS = [
  'aplicacao', 'aplicacoes', 'software', 'parametrizacao', 'parametro', 'regra de negocio', 'regra negocio', 'job', 'jobs',
  'query', 'bug', 'codigo', 'deploy', 'integracao', 'interface', 'api', 'cadastro', 'dados', 'rotina', 'processamento',
  'configuracao', 'performance aplicacao', 'erro sistemico'
];
// Frases fortes na Descrição do Ofensor (usadas só quando nenhum campo estruturado decidiu)
const OLX_INFRA_PHRASES = [
  'queda de energia', 'falta de energia', 'oscilacao de energia', 'queda de link', 'link de internet', 'link da operadora',
  'indisponibilidade do link', 'falha no switch', 'falha de switch', 'firewall', 'falha de rede', 'problema de rede',
  'lentidao na rede', 'queda de rede', 'servidor fisico', 'falha de hardware', 'storage', 'access point'
];

function olxAnyKeyword(text, list) {
  const t = olFold(text);
  if (!t) return '';
  for (let i = 0; i < list.length; i++) if (olMatchKeyword(t, list[i])) return list[i];
  return '';
}
function olxMs(v) { return olIsDate(v) ? v.getTime() : null; }
function olxTxt(v, max) {
  const s = String(v === null || v === undefined ? '' : v).replace(/\s+\n/g, '\n').trim();
  return max && s.length > max ? s.slice(0, max) + '…' : s;
}
function olxSheetValues(ss, name) {
  const sh = ss.getSheetByName(name);
  if (!sh) return null;
  const lr = sh.getLastRow(), lc = sh.getLastColumn();
  if (lr < 1 || lc < 1) return [];
  return sh.getRange(1, 1, lr, lc).getValues();
}
function olxPrbList(raw) {
  const out = [];
  String(raw === null || raw === undefined ? '' : raw).toUpperCase().replace(/PRB\d+/g, m => { if (out.indexOf(m) === -1) out.push(m); return m; });
  return out;
}
function olxIsClosedState(s) {
  const f = olFold(s);
  return /closed|complete|resolved|cancel|encerrad|fechad|concluid|finaliz/.test(f);
}

/** Aba opcional OL_Camada (A: Tipo = Incidente | Ofensor | Tecnologia | Grupo; B: Valor; C: Camada = Aplicação | Infraestrutura). */
function olxReadLayerOverrides(ss) {
  const out = { found: false, incidente: {}, ofensor: {}, tecnologia: {}, grupo: {}, rows: 0 };
  const values = olxSheetValues(ss, OLX_LAYER_SHEET);
  if (!values) return out;
  out.found = true;
  for (let i = 0; i < values.length; i++) {
    const tipo = olFold(values[i][0]), valor = olFold(values[i][1]), camada = olFold(values[i][2]);
    if (!tipo || !valor || !camada || tipo === 'tipo') continue;
    const layer = camada.indexOf('infra') === 0 ? 'infra' : (camada.indexOf('aplica') === 0 || camada === 'app' ? 'app' : '');
    if (!layer) continue;
    const key = tipo.indexOf('incid') === 0 ? 'incidente' : tipo.indexOf('ofens') === 0 ? 'ofensor' : tipo.indexOf('tecno') === 0 ? 'tecnologia' : tipo.indexOf('grupo') === 0 ? 'grupo' : '';
    if (!key) continue;
    out[key][key === 'incidente' ? valor.toUpperCase() : valor] = layer;
    out.rows++;
  }
  return out;
}

/**
 * Camada do Incidente: Aplicação ou Infraestrutura. Ordem dos sinais (o primeiro que decidir vale):
 *  1. aba OL_Camada (por Incidente, Ofensor, Tecnologia impactada ou Grupo);
 *  2. termos de Infraestrutura no Ofensor ou na Tecnologia impactada (inclui datacenter, rede, energia);
 *  3. termos inequívocos de datacenter (Equinix, FEX, Nexus...) no Título ou nas Descrições;
 *  4. Ofensor com termo de Aplicação;
 *  5. Category / Subcategory do ServiceNow;
 *  6. Grupo responsável (ServiceNow ou da Mudança);
 *  7. frases fortes na Descrição do Ofensor (queda de energia, link, switch, datacenter...);
 *  8. padrão: Aplicação.
 */
function olxClassifyLayer(I, ov) {
  const done = (layer, why, src) => ({ layer: layer, why: why, src: src });
  if (ov.incidente[I.id]) return done(ov.incidente[I.id], 'Incidente listado na aba ' + OLX_LAYER_SHEET, 'planilha');
  const ofensores = [I.ofe].concat(I.ofeRows || []).filter(Boolean);
  for (let i = 0; i < ofensores.length; i++) {
    const k = olFold(ofensores[i]);
    if (ov.ofensor[k]) return done(ov.ofensor[k], 'Ofensor "' + ofensores[i] + '" na aba ' + OLX_LAYER_SHEET, 'planilha');
  }
  for (let i = 0; i < I.tImp.length; i++) {
    const k = olFold(I.tImp[i]);
    if (ov.tecnologia[k]) return done(ov.tecnologia[k], 'Tecnologia "' + I.tImp[i] + '" na aba ' + OLX_LAYER_SHEET, 'planilha');
  }
  const groups = [I.sn && I.sn.group, I.chgInfo && I.chgInfo.group].filter(g => g && g !== 'N/A');
  for (let i = 0; i < groups.length; i++) {
    const k = olFold(groups[i]);
    if (ov.grupo[k]) return done(ov.grupo[k], 'Grupo "' + groups[i] + '" na aba ' + OLX_LAYER_SHEET, 'planilha');
  }
  const isChange = ofensores.some(o => olFold(o) === 'mudanca');
  // 1º) termos de Infraestrutura no Ofensor e na Tecnologia impactada
  for (let i = 0; i < ofensores.length; i++) {
    if (olFold(ofensores[i]) === 'mudanca') continue; // Mudança decide pela Category/Grupo da SM
    const kwI = olxAnyKeyword(ofensores[i], OLX_INFRA_KEYWORDS);
    if (kwI) return done('infra', 'Ofensor "' + ofensores[i] + '" (termo "' + kwI + '")', 'regra');
  }
  for (let i = 0; i < I.tImp.length; i++) {
    const kwT = olxAnyKeyword(I.tImp[i], OLX_INFRA_KEYWORDS);
    if (kwT) return done('infra', 'Tecnologia impactada "' + I.tImp[i] + '" (termo "' + kwT + '")', 'regra');
  }
  // 2º) termos inequívocos de datacenter no Título e nas Descrições
  const texts = [['Título', I.tit], ['Descrição do Ofensor', I.dOf], ['Descrição do Impacto', I.dImp]];
  for (let i = 0; i < texts.length; i++) {
    const kwS = olxAnyKeyword(texts[i][1], OLX_INFRA_STRONG);
    if (kwS) return done('infra', texts[i][0] + ' menciona "' + kwS + '"', 'regra');
  }
  // 3º) Ofensor claramente de Aplicação
  for (let i = 0; i < ofensores.length; i++) {
    if (olFold(ofensores[i]) === 'mudanca') continue;
    const kwA = olxAnyKeyword(ofensores[i], OLX_APP_KEYWORDS);
    if (kwA) return done('app', 'Ofensor "' + ofensores[i] + '" (termo "' + kwA + '")', 'regra');
  }
  const cat = [I.sn && I.sn.category, I.sn && I.sn.subcategory, I.chgInfo && I.chgInfo.category].filter(Boolean).join(' / ');
  if (cat) {
    const kwI = olxAnyKeyword(cat, OLX_INFRA_KEYWORDS.concat(['hardware', 'network', 'infrastructure']));
    if (kwI) return done('infra', 'Category do ServiceNow "' + cat + '"', 'regra');
    const kwA = olxAnyKeyword(cat, ['software', 'application', 'aplicacao', 'sistema']);
    if (kwA && !isChange) return done('app', 'Category do ServiceNow "' + cat + '"', 'regra');
  }
  for (let i = 0; i < groups.length; i++) {
    const kw = olxAnyKeyword(groups[i], OLX_INFRA_KEYWORDS);
    if (kw) return done('infra', 'Grupo "' + groups[i] + '" (termo "' + kw + '")', 'regra');
  }
  const ph = olxAnyKeyword(I.dOf, OLX_INFRA_PHRASES.concat(['datacenter', 'data center']));
  if (ph) return done('infra', 'Descrição do Ofensor menciona "' + ph + '"', 'regra');
  return done('app', isChange ? 'Mudança sem indício de Infraestrutura (Category/Grupo)' : 'Sem indício de Infraestrutura nos campos', 'padrão');
}

/** MajorIncidentes[Ano] completo (A..Y) só para os incidentes pedidos. */
function olxReadMajorIncidents(ss, year, idSet) {
  const map = {};
  const values = olxSheetValues(ss, 'MajorIncidentes' + year);
  if (!values) return map;
  for (let i = 1; i < values.length; i++) {
    const r = values[i];
    const id = String(r[0] || '').trim();
    if (!id || !idSet[id]) continue;
    map[id] = {
      jornada: olxTxt(r[COL_JORNADA]), titulo: olxTxt(r[6], 200), impacto: olxTxt(r[7], 500), abrangencia: olxTxt(r[COL_ABRANGENCIA]),
      dOf: olxTxt(r[10], 600), sol: olxTxt(r[11], 400), prbs: olxPrbList(r[COL_PROBLEMA]), tImp: olxTxt(r[COL_TECH_IMPACTADA]),
      ofe: olxTxt(r[COL_OFENSOR]), obs: olxTxt(r[17], 300), sintoma: olxTxt(r[20]), typeSm: olxTxt(r[COL_TYPE_SM]).toUpperCase(),
      sm: normalizeChangeNumber(r[COL_SM_NUMBER]), ab: olxMs(r[COL_ABERTURA]), enc: olxMs(r[3])
    };
  }
  return map;
}

/** Major_ServiceNow (A..AB) só para os incidentes pedidos. */
function olxReadServiceNow(ss, idSet) {
  const map = {};
  const values = olxSheetValues(ss, 'Major_ServiceNow');
  if (!values) return map;
  for (let i = 1; i < values.length; i++) {
    const r = values[i];
    const id = String(r[MSN_COL_NUMBER] || '').trim();
    if (!id || !idSet[id]) continue;
    map[id] = {
      caller: olxTxt(r[MSN_COL_CALLER]), priority: olxTxt(r[MSN_COL_PRIORITY]), group: olxTxt(r[MSN_COL_ASSIGNMENT_GROUP]) || 'N/A',
      service: olxTxt(r[MSN_COL_SERVICE]) || 'N/A', offering: olxTxt(r[MSN_COL_SERVICE_OFFERING]) || 'N/A',
      category: olxTxt(r[20]), subcategory: olxTxt(r[21]), resolutionCode: olxTxt(r[25]),
      prbs: olxPrbList(r[MSN_COL_PROBLEM]), causedByChange: olxTxt(r[6]), chg: normalizeChangeNumber(r[MSN_COL_CHANGE_REQUEST]),
      opened: olxMs(r[MSN_COL_OPENED]), resolved: olxMs(r[23])
    };
  }
  return map;
}

/**
 * Problemas ligados aos incidentes de O&L: MajorProblems/MajorProblems2026 (status, área, datas, risco aceito),
 * Problem_ServiceNow (statement, estado, grupo, notas de causa/correção) e as tarefas PTASK_RCA / PTASK_IMP.
 * Causa do Problema e Processo de Origem vêm só da tarefa de RCA (PTASK_RCA: Cause Problem e Source process).
 * Estado, abertura, fechamento e equipe de cada tarefa vêm da RCATask_List quando a tarefa está lá (mesma regra
 * do Engajamento por Equipe da Visão Geral: fechada = state "closed"); senão, das próprias abas PTASK.
 */
function olxReadProblems(ss, incSet, prbSeed) {
  const prbSet = {};
  Object.keys(prbSeed).forEach(p => { prbSet[p] = true; });
  const byInc = {};
  const link = (inc, prb) => {
    if (!inc || !prb) return;
    prbSet[prb] = true;
    (byInc[inc] = byInc[inc] || []);
    if (byInc[inc].indexOf(prb) === -1) byInc[inc].push(prb);
  };
  const problems = {};
  const P = prb => (problems[prb] = problems[prb] || { id: prb, incidents: [], tasks: [] });

  // 1) Acompanhamento GPROB
  ['MajorProblems2026', 'MajorProblems'].forEach(name => {
    const values = olxSheetValues(ss, name);
    if (!values) return;
    for (let i = 1; i < values.length; i++) {
      const r = values[i];
      const inc = String(r[0] || '').trim();
      const prbs = olxPrbList(r[1]);
      if (!prbs.length) continue;
      const rel = incSet[inc];
      prbs.forEach(prb => {
        if (!rel && !prbSet[prb]) return;
        if (rel) link(inc, prb);
        const p = P(prb);
        if (inc && p.incidents.indexOf(inc) === -1) p.incidents.push(inc);
        if (!p.gprob) {
          p.gprob = {
            sheet: name, status: olxTxt(r[2]), resp: olxTxt(r[3]), area: olxTxt(r[4]) || 'N/A',
            dataInc: olxMs(r[5]), opened: olxMs(r[6]), closed: olxMs(r[7]),
            aging: typeof r[10] === 'number' ? r[10] : null, riscoAceito: olxTxt(r[11])
          };
        }
      });
    }
  });

  // 2) Problem_ServiceNow
  const psn = olxSheetValues(ss, 'Problem_ServiceNow');
  if (psn) {
    for (let i = 1; i < psn.length; i++) {
      const r = psn[i];
      const prb = olxPrbList(r[PSN_COL_NUMBER])[0];
      if (!prb) continue;
      const inc = String(r[2] || '').trim();
      if (incSet[inc]) link(inc, prb);
      if (!prbSet[prb]) continue;
      const p = P(prb);
      if (inc && incSet[inc] && p.incidents.indexOf(inc) === -1) p.incidents.push(inc);
      p.sn = {
        opened: olxMs(r[3]), statement: olxTxt(r[PSN_COL_STATEMENT], 300), priority: olxTxt(r[PSN_COL_PRIORITY]),
        state: olxTxt(r[PSN_COL_STATE]), group: olxTxt(r[PSN_COL_ASSIGNMENT_GROUP]) || 'N/A',
        service: olxTxt(r[10]), offering: olxTxt(r[11]), causeNotes: olxTxt(r[12], 500), workaround: olxTxt(r[13], 300),
        fixNotes: olxTxt(r[14], 400), resolutionCode: olxTxt(r[15])
      };
    }
  }

  // 3) RCATask_List: estado, abertura, fechamento e equipe de cada tarefa (base do Engajamento por Equipe)
  const rtl = {};
  const rtlValues = olxSheetValues(ss, 'RCATask_List');
  const rtlDate = v => {
    if (olIsDate(v)) return v.getTime();
    const m = String(v || '').trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
    return m ? new Date(+m[3], +m[2] - 1, +m[1], +(m[4] || 0), +(m[5] || 0), +(m[6] || 0)).getTime() : null;
  };
  if (rtlValues) {
    for (let i = 1; i < rtlValues.length; i++) {
      const r = rtlValues[i];
      const num = String(r[0] || '').trim();
      if (!num || rtl[num]) continue;
      rtl[num] = { state: olxTxt(r[1]), opened: rtlDate(r[2]), closed: rtlDate(r[3]), group: olxTxt(r[6]) || 'N/A', taskType: olxTxt(r[10]) };
    }
  }

  // 4) Tarefas do Problema
  const readTasks = (name, type) => {
    const values = olxSheetValues(ss, name);
    if (!values) return;
    for (let i = 1; i < values.length; i++) {
      const r = values[i];
      const prb = olxPrbList(r[PTASK_COL_PROBLEM])[0];
      const inc = String(r[3] || '').trim();
      if (prb && incSet[inc]) link(inc, prb);
      if (!prb || !prbSet[prb]) continue;
      const num = olxTxt(r[PTASK_COL_NUMBER]);
      const L = rtl[num] || null;
      const closed = L ? olFold(L.state) === 'closed' : olxIsClosedState(r[PTASK_COL_STATE]);
      P(prb).tasks.push({
        num: num, type: type, desc: olxTxt(r[PTASK_COL_SHORT_DESC], 220), taskType: (L && L.taskType) || olxTxt(r[PTASK_COL_TYPE]),
        state: (L && L.state) || olxTxt(r[PTASK_COL_STATE]), closed: closed, src: L ? 'RCATask_List' : name,
        group: (L && L.group !== 'N/A' ? L.group : olxTxt(r[PTASK_COL_ASSIGNMENT_GROUP])) || 'N/A',
        opened: L && L.opened ? L.opened : olxMs(r[19]), closedAt: L ? L.closed : (closed ? olxMs(r[21]) : null),
        causa: type === 'RCA' ? olxTxt(r[12]) : '', processo: type === 'RCA' ? olxTxt(r[13]) : '',
        plano: olxTxt(r[15], 500), due: olxMs(r[17])
      });
    }
  };
  readTasks('PTASK_RCA', 'RCA');
  readTasks('PTASK_IMP', 'IMP');

  Object.keys(byInc).forEach(inc => byInc[inc].forEach(prb => { const p = P(prb); if (p.incidents.indexOf(inc) === -1) p.incidents.push(inc); }));
  // Só os Problemas ligados a algum incidente de O&L
  Object.keys(problems).forEach(prb => { if (!problems[prb].incidents.some(inc => incSet[inc])) delete problems[prb]; });
  return { byInc: byInc, problems: problems, rtlFound: !!rtlValues };
}

/** Change_MI só para as Mudanças pedidas. */
function olxReadChanges(ss, chgSet) {
  const map = {};
  const values = olxSheetValues(ss, 'Change_MI');
  if (!values) return map;
  for (let i = 1; i < values.length; i++) {
    const r = values[i];
    const num = normalizeChangeNumber(r[CHG_COL_NUMBER]);
    if (!num || !chgSet[num]) continue;
    map[num] = {
      num: num, template: olxTxt(r[1]), desc: olxTxt(r[2], 220), type: olxTxt(r[4]), state: olxTxt(r[6]),
      service: olxTxt(r[CHG_COL_SERVICE]) || 'N/A', offering: olxTxt(r[CHG_COL_SERVICE_OFFERING]) || 'N/A',
      start: olxMs(r[CHG_COL_PLANNED_START]), end: olxMs(r[CHG_COL_PLANNED_END]), group: olxTxt(r[CHG_COL_ASSIGNMENT_GROUP]) || 'N/A',
      risk: olxTxt(r[19]), category: olxTxt(r[20]), unav: olxTxt(r[22])
    };
  }
  return map;
}

/**
 * Ponto de entrada da sub-visão. Devolve o ano corrente (ou o pedido) e o anterior, já enriquecidos.
 * Só entram incidentes de O&L com origem Tecnologia (regra da Visão O&L); a contagem total da aba fica
 * em meta.reconcile para conferência. Datas em milissegundos.
 */
function getOLAnaliseData(year) {
  try {
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
    const years = olListYears(ss);
    const statusInfo = olReadStatusApuracao(ss);
    const nowY = new Date().getFullYear();
    year = Number(year) || (years.indexOf(nowY) !== -1 ? nowY : (years[0] || nowY));
    const callerMap = buildCallerMap(ss);
    const overrides = olxReadLayerOverrides(ss);
    const warnings = statusInfo.warnings.slice();

    let lastUpdated = 'N/A';
    const sheetConfig = ss.getSheetByName(SHEET_CONFIG);
    if (sheetConfig) {
      const val = sheetConfig.getRange('B4').getValue();
      if (olIsDate(val)) lastUpdated = val.toLocaleString('pt-BR');
    }

    const yearsToRead = [year, year - 1];
    const raw = {};
    const incSet = {};
    yearsToRead.forEach(y => {
      const imp = olReadImpactos(ss, y, callerMap);
      raw[y] = imp;
      if (!imp.found) warnings.push('Aba ' + OL_IMPACT_SHEET_PREFIX + y + ' não encontrada.');
      Object.keys(imp.incidents || {}).forEach(id => { if (imp.incidents[id].tec) incSet[id] = true; });
    });

    const sn = olxReadServiceNow(ss, incSet);
    const miByYear = {};
    yearsToRead.forEach(y => { miByYear[y] = olxReadMajorIncidents(ss, y, incSet); });

    // Problemas e Mudanças citados pelos incidentes
    const prbSeed = {}, chgSet = {};
    yearsToRead.forEach(y => {
      (raw[y].rows || []).forEach(r => { if (incSet[r.inc]) olxPrbList(r.prb).forEach(p => { prbSeed[p] = true; }); });
      Object.keys(miByYear[y]).forEach(id => { miByYear[y][id].prbs.forEach(p => { prbSeed[p] = true; }); if (miByYear[y][id].sm) chgSet[miByYear[y][id].sm] = true; });
    });
    Object.keys(sn).forEach(id => { sn[id].prbs.forEach(p => { prbSeed[p] = true; }); if (sn[id].chg) chgSet[sn[id].chg] = true; });
    const prob = olxReadProblems(ss, incSet, prbSeed);
    const changes = olxReadChanges(ss, chgSet);

    const out = {};
    yearsToRead.forEach(y => {
      const imp = raw[y];
      const rows = [], incidents = {};
      let totalAll = 0, totalTec = 0;
      Object.keys(imp.incidents || {}).forEach(id => { totalAll++; if (imp.incidents[id].tec) totalTec++; });
      (imp.rows || []).forEach(r => {
        if (!r.tec) return;
        rows.push({
          inc: r.inc, st: r.st, mk: r.mk, loc: r.loc, imp: r.imp, sin: r.sin, sev: r.sev, m: r.m, ab: r.ab, dur: r.dur, durCd: r.durCd,
          apd: r.apd, card: r.card, flow: r.flow, ent: r.ent, entName: r.entName, entKind: r.entKind, entCc: r.entCc,
          entUnnamed: r.entUnnamed, unm: r.unm, tImp: r.tImp, ofe: r.ofe
        });
        let I = incidents[r.inc];
        if (!I) {
          const base = imp.incidents[r.inc] || {};
          const M = miByYear[y][r.inc] || null;
          const S = sn[r.inc] || null;
          I = incidents[r.inc] = {
            id: r.inc, year: y, sev: isFinite(base.sev) && base.sev !== null ? base.sev : r.sev, ttr: base.ttr, ttrTxt: base.ttrTxt || '',
            foundMI: !!base.foundMI, origem: base.origem || 'N/A', streams: [], ab: null, enc: null, m: null,
            tit: '', dImp: '', dOf: '', sol: '', tImp: [], ofeRows: [], ofe: M && M.ofe ? M.ofe : '', prbs: [],
            jornada: M ? M.jornada : '', sintoma: M ? M.sintoma : '', typeSm: M ? M.typeSm : '', sm: (M && M.sm) || (S && S.chg) || '',
            obs: M ? M.obs : '',
            sn: S ? { priority: S.priority, group: S.group, service: S.service, offering: S.offering, category: S.category, subcategory: S.subcategory, resolutionCode: S.resolutionCode, causedByChange: S.causedByChange, opened: S.opened, resolved: S.resolved } : null
          };
          if (M && M.tImp) I.tImp.push(M.tImp);
          if (M && M.dOf) I.dOf = M.dOf;
          if (M && M.sol) I.sol = M.sol;
          if (M && M.titulo) I.tit = M.titulo;
          if (M && M.ab) I.abMI = M.ab;
          (M ? M.prbs : []).concat(S ? S.prbs : []).concat(prob.byInc[r.inc] || []).forEach(p => { if (I.prbs.indexOf(p) === -1) I.prbs.push(p); });
        }
        if (I.streams.indexOf(r.st) === -1) I.streams.push(r.st);
        if (r.ab !== null && (I.ab === null || r.ab < I.ab)) { I.ab = r.ab; I.m = r.m; }
        if (I.m === null && r.m) I.m = r.m;
        const enc = r.ab !== null && r.dur ? r.ab + r.dur * 60000 : null;
        if (enc && (I.enc === null || enc > I.enc)) I.enc = enc;
        if (r.tImp && r.tImp !== 'N/A' && I.tImp.indexOf(r.tImp) === -1) I.tImp.push(r.tImp);
        if (r.ofe && r.ofe !== 'N/A' && I.ofeRows.indexOf(r.ofe) === -1) I.ofeRows.push(r.ofe);
        olxPrbList(r.prb).forEach(p => { if (I.prbs.indexOf(p) === -1) I.prbs.push(p); });
      });
      // Descrições vindas da aba de Impactos quando a MajorIncidentes não tem
      (imp.rows || []).forEach(r => {
        const I = incidents[r.inc];
        if (!I) return;
        if (!I.tit && r.tit) I.tit = r.tit;
        if (!I.dImp && r.dImp) I.dImp = r.dImp;
        if (!I.dOf && r.dOf) I.dOf = r.dOf;
        if (!I.sol && r.sol) I.sol = r.sol;
      });
      Object.keys(incidents).forEach(id => {
        const I = incidents[id];
        if (!I.ofe) I.ofe = I.ofeRows[0] || 'N/A';
        if (!I.tImp.length) I.tImp.push('N/A');
        I.chgInfo = I.sm && changes[I.sm] ? changes[I.sm] : null;
        const L = olxClassifyLayer(I, overrides);
        I.layer = L.layer; I.layerWhy = L.why; I.layerSrc = L.src;
        delete I.chgInfo; // Mudança vai em changes (evita duplicar no payload)
      });
      out[y] = {
        year: y,
        statuses: olStatusForYear(statusInfo, y),
        impactsFound: !!imp.found,
        rows: rows,
        incidents: incidents,
        reconcile: { incidentsAll: totalAll, incidentsTec: totalTec },
        disponibilidade: olReadDisponibilidade(ss, y),
        apuracao: olReadApuracao(ss, y)
      };
    });

    return {
      ok: true,
      year: year,
      prevYear: year - 1,
      availableYears: years,
      today: new Date().getTime(),
      lastUpdated: lastUpdated,
      statusSource: statusInfo.found ? 'SHEET' : 'FALLBACK',
      warnings: warnings,
      current: out[year],
      previous: out[year - 1],
      problems: prob.problems,
      changes: changes,
      layerOverrides: { found: overrides.found, rows: overrides.rows, sheet: OLX_LAYER_SHEET },
      rcaTaskListFound: prob.rtlFound,
      config: {
        flows: OL_FLOW_DISPLAY_ORDER.map(k => {
          const f = OL_FLOWS.find(x => x.key === k);
          return { key: f.key, label: f.label, short: f.short, icon: f.icon, entityType: f.entityType };
        }),
        catalog: OL_CATALOGO,
        targets: {
          olaSev0: OLA_TARGET_SEV0_MIN, olaSev1: OLA_TARGET_SEV1_MIN,
          mttrSev0: OLX_MTTR_TARGET_SEV0_MIN, mttrSev1: OLX_MTTR_TARGET_SEV1_MIN,
          mttrSemOutliersSev0: OLX_MTTR_TARGET_SEV0_MIN, mttrSemOutliersSev1: OLX_MTTR_TARGET_SEV1_MIN,
          outlierSev0: MTTR_OUTLIER_SEV0_MIN, outlierSev1: MTTR_OUTLIER_SEV1_MIN
        }
      }
    };
  } catch (e) {
    return { ok: false, error: e.toString(), stack: e.stack ? String(e.stack).slice(0, 800) : '' };
  }
}
