// ARQUIVO: Monitoramento.gs

var PROPERTY_PREFIX = 'LAST_ROW_COUNT_';
var CONFIG_CELL = "B4";
var SHEET_CONFIG_MON = "Configuracao";
var SPREADSHEET_ID_MONITOR = '1WUCKPAUHWidLFc0PpVgAzehQnEfSSZLFx1qdGO_Y24Y';

// List of sheets to monitor for changes
const SHEETS_TO_MONITOR = [
  "MajorIncidentes2025",
  "MajorProblems",
  "MP_List",
  "RCATask_List"
];

/**
 * Setup do monitoramento.
 */
function setupMonitor() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID_MONITOR);

  // Limpa gatilhos antigos
  const allTriggers = ScriptApp.getProjectTriggers();
  for (let i = 0; i < allTriggers.length; i++) {
    let trigger = allTriggers[i];
    if (trigger.getHandlerFunction() === 'checkNewRows') {
      ScriptApp.deleteTrigger(trigger);
    }
  }

  // Cria Novo Trigger (a cada 5 minutos)
  ScriptApp.newTrigger('checkNewRows')
      .timeBased()
      .everyMinutes(5)
      .create();
      
  Logger.log('Monitoramento Multi-Aba Configurado.');
}

/**
 * Função de verificação: Checa se houve alteração no número de linhas de qualquer aba monitorada
 */
function checkNewRows() {
  try {
    const ss = SpreadsheetApp.openById(SPREADSHEET_ID_MONITOR);
    const configSheet = ss.getSheetByName(SHEET_CONFIG_MON);
    if (!configSheet) return;

    const props = PropertiesService.getScriptProperties();
    let anyChange = false;

    SHEETS_TO_MONITOR.forEach(sheetName => {
      const sheet = ss.getSheetByName(sheetName);
      if (!sheet) return;

      const currentRows = sheet.getLastRow();
      const propertyKey = PROPERTY_PREFIX + sheetName;
      const savedRowsStr = props.getProperty(propertyKey);
      const savedRows = savedRowsStr ? parseInt(savedRowsStr) : 0;

      if (currentRows !== savedRows) {
        Logger.log(`Alteração detectada na aba ${sheetName}: ${savedRows} -> ${currentRows}`);
        anyChange = true;
        // Atualiza o estado salvo para esta aba
        props.setProperty(propertyKey, String(currentRows));
      }
    });

    // Se houve qualquer mudança em qualquer uma das abas, atualiza o timestamp central
    if (anyChange) {
      Logger.log("Sinalizando atualização do Dashboard na aba Configuracao.");
      configSheet.getRange(CONFIG_CELL).setValue(new Date());
    }

  } catch (e) {
    Logger.log("Erro Monitoramento: " + e.toString());
  }
}
// End of File