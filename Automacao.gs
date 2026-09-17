// ID da Planilha Destino (Igual à do Dashboard)
const SPREADSHEET_ID_AUTO = '1WUCKPAUHWidLFc0PpVgAzehQnEfSSZLFx1qdGO_Y24Y';
const DEST_SHEET_NAME = 'MP_List';
const DEST_SHEET_TASKS = 'RCATask_List';
const LABEL_PROCESSED = 'Processado_Automatizado';

// Search Queries
const SEARCH_QUERY_MP = 'subject:"Major Problems Opened After 01/01/2025" -label:Processado_Automatizado';
const SEARCH_QUERY_TASKS = 'subject:"Major Problem Tasks Opened After 01/01/2025" -label:Processado_Automatizado';

/**
 * Função Principal 1: Processa Emails de Major Problems
 */
function processMajorProblemsEmail() {
  processEmailAutomation(SEARCH_QUERY_MP, DEST_SHEET_NAME);
}

/**
 * Função Principal 2: Processa Emails de Major Problem Tasks
 */
function processMajorProblemTasksEmail() {
  processEmailAutomation(SEARCH_QUERY_TASKS, DEST_SHEET_TASKS);
}

/**
 * Core Logic: Generic function to process emails and import CSV
 */
function processEmailAutomation(query, sheetName) {
  try {
    const threads = GmailApp.search(query);
    if (threads.length === 0) {
      Logger.log(`Nenhum email novo encontrado para: ${query}`);
      return;
    }

    const ss = SpreadsheetApp.openById(SPREADSHEET_ID_AUTO);
    let sheet = ss.getSheetByName(sheetName);
    
    if (!sheet) {
      sheet = ss.insertSheet(sheetName);
    }

    let label = GmailApp.getUserLabelByName(LABEL_PROCESSED);
    if (!label) {
      label = GmailApp.createLabel(LABEL_PROCESSED);
    }

    threads.forEach(thread => {
      const messages = thread.getMessages();
      const message = messages[messages.length - 1]; // Last message in thread
      const attachments = message.getAttachments();
      
      let csvContent = "";
      let foundCsv = false;

      for (let att of attachments) {
        if (att.getContentType() === "text/csv" || att.getName().toLowerCase().endsWith(".csv")) {
          // Explicitly use ISO-8859-1 for standard CSV exports with Portuguese characters
          csvContent = att.getDataAsString("ISO-8859-1");
          foundCsv = true;
          break;
        }
      }

      if (foundCsv && csvContent) {
        importCsvToSheet(sheet, csvContent);
        Logger.log(`Email importado com sucesso para ${sheetName}.`);
        thread.addLabel(label);
        thread.markRead(); // Mark as read
        thread.moveToArchive();
      } else {
        Logger.log(`Email para ${sheetName} sem anexo CSV válido.`);
      }
    });

  } catch (e) {
    Logger.log(`Erro em processEmailAutomation (${sheetName}): ` + e.toString());
  }
}

/**
 * Helper: Importa string CSV para a Planilha
 */
function importCsvToSheet(sheet, csvString) {
  const csvData = Utilities.parseCsv(csvString);
  if (csvData.length > 0) {
    // Limpa os dados existentes para evitar duplicidade,
    // já que o ServiceNow envia a extração full
    sheet.clearContents();
    
    // Importa todo o conteúdo do CSV (incluindo cabeçalho) começando da linha 1
    // ola
    sheet.getRange(1, 1, csvData.length, csvData[0].length).setValues(csvData);
    
    Logger.log(`Dados na guia ${sheet.getName()} foram substituídos com sucesso.`);
  }
}

/**
 * Setup do Trigger Diário (1h e 2h da manhã respectivamente)
 */
function setupAutomationTrigger() {
  const triggers = ScriptApp.getProjectTriggers();
  const functionsToTrigger = ['processMajorProblemsEmail', 'processMajorProblemTasksEmail'];

  // Limpa gatilhos antigos
  for (let t of triggers) {
    if (functionsToTrigger.includes(t.getHandlerFunction())) {
      ScriptApp.deleteTrigger(t);
    }
  }

  // Cria novos gatilhos
  ScriptApp.newTrigger('processMajorProblemsEmail')
    .timeBased()
    .everyDays(1)
    .atHour(1)
    .create();

  ScriptApp.newTrigger('processMajorProblemTasksEmail')
    .timeBased()
    .everyDays(1)
    .atHour(2) // 2 AM to avoid overlap if many emails
    .create();
    
  Logger.log("Triggers de Automação (MP e Tasks) Configurados.");
}