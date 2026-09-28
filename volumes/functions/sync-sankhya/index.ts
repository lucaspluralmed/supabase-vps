import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

function parseSankhyaDate(dateStr: string, asTimestamp = false): string | null {
  if (!dateStr || dateStr.trim() === '') return null;
  const day = dateStr.substring(0, 2);
  const month = dateStr.substring(2, 4);
  const year = dateStr.substring(4, 8);
  const time = dateStr.substring(9);
  
  if (asTimestamp) {
    return `${year}-${month}-${day}T${time}`;
  }
  return `${year}-${month}-${day}`;
}

serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  try {
    const authHeader = req.headers.get('Authorization');
    // Sem fallback literal: um segredo versionado no git nao e segredo.
    // Se CRON_SECRET nao estiver configurado, o caminho do cron fica
    // desabilitado em vez de virar "Bearer " (que qualquer um enviaria).
    const cronSecret = Deno.env.get("CRON_SECRET") ?? "";
    
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
    const supabase = createClient(supabaseUrl, supabaseKey);

    let isAuthorized = false;
    if (cronSecret && authHeader === `Bearer ${cronSecret}`) {
      isAuthorized = true;
    } else if (authHeader) {
      const jwt = authHeader.replace('Bearer ', '');
      const { data: { user } } = await supabase.auth.getUser(jwt);
      if (user) {
        // Espelha o gate da UI (DashboardLayout: canSync): so admin ativo ou
        // quem tem a tela 'sincronizacao' pode disparar uma carga completa.
        const { data: perfil } = await supabase
          .from('profiles')
          .select('role, status, telas_acesso')
          .eq('id', user.id)
          .single();
        isAuthorized = !!perfil
          && perfil.status === 'active'
          && (perfil.role === 'admin' || (perfil.telas_acesso ?? []).includes('sincronizacao'));
      }
    }

    if (!isAuthorized) {
      return new Response(JSON.stringify({ error: "Não autorizado." }), {
        status: 401,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }

    let reqBody: any = {};
    if (req.method === 'POST' && req.headers.get('Content-Type')?.includes('application/json')) {
      try {
        reqBody = await req.json();
      } catch (e) {
        reqBody = {};
      }
    }
    const { start_date, end_date, mode = 'incremental' } = reqBody;

    const sankhyaToken = Deno.env.get("SANKHYA_API_KEY");
    if (!sankhyaToken) {
      throw new Error("SANKHYA_API_KEY não encontrada.");
    }

    const writeLog = async (message: string, level: string = 'info') => {
      console.log(`[${level.toUpperCase()}] ${message}`);
      await supabase.from('sync_logs').insert({ message, level, sync_type: mode });
    };

    // Apaga logs antigos da tabela de forma segura
    const { error: clearError } = await supabase.from('sync_logs').delete().neq('id', '00000000-0000-0000-0000-000000000000');
    if (clearError) console.error("Erro ao limpar logs", clearError);
    
    const loadParceirosMap = async () => {
      try {
        const res = await fetch("https://api-sankhya.pluralmed.com.br/api/query", {
          method: "POST",
          headers: { "Authorization": `Bearer ${sankhyaToken}`, "ApiKey": sankhyaToken, "Content-Type": "application/json" },
          body: JSON.stringify({
            body: {
              requestBody: { sql: "SELECT CODPARC, NOMEPARC, RAZAOSOCIAL FROM TGFPAR WHERE CODPARC IS NOT NULL" },
              serviceName: "DbExplorerSP.executeQuery"
            },
            module: "mge",
            service_name: "string"
          })
        });
        const json = await res.json();
        const responseData = Array.isArray(json) ? json[0] : json;
        const pRows = responseData?.data?.responseBody?.rows || [];
        const map = new Map<number, string>();
        pRows.forEach((r: any[]) => {
          const cod = Number(r[0]);
          const nome = (r[2] && String(r[2]).trim()) || (r[1] && String(r[1]).trim()) || `Parceiro ${cod}`;
          map.set(cod, nome);
        });
        return map;
      } catch (err) {
        console.warn("Falha ao pré-carregar TGFPAR:", err);
        return new Map<number, string>();
      }
    };

    const parceirosMap = await loadParceirosMap();

    // MODO RESYNC ALL (BAIXA TODAS AS COLUNAS DESDE O INÍCIO, PAGINADO)
    if (mode === 'resync_all') {
      await writeLog("Iniciando Resync Completo (Buscando todas as colunas a partir de 2025-01-01)...");
      
      let hasMore = true;
      let lastId = 0;
      let pageCount = 0;
      let totalInserted = 0;

      while (hasMore) {
        pageCount++;
        await writeLog(`Buscando página ${pageCount} (ID > ${lastId})...`);
        
        const sqlQuery = `SELECT * FROM VW_FINANCEIRO_FULL WHERE DATA_COMPETENCIA >= TO_DATE('2025-01-01', 'YYYY-MM-DD') AND ID > ${lastId} ORDER BY ID ASC`;

        const sankhyaRes = await fetch("https://api-sankhya.pluralmed.com.br/api/query", {
          method: "POST",
          headers: { "Authorization": `Bearer ${sankhyaToken}`, "ApiKey": sankhyaToken, "Content-Type": "application/json" },
          body: JSON.stringify({ body: { requestBody: { sql: sqlQuery }, serviceName: "DbExplorerSP.executeQuery" }, module: "mge", service_name: "string" })
        });

        const sankhyaJson = await sankhyaRes.json();
        const responseData = Array.isArray(sankhyaJson) ? sankhyaJson[0] : sankhyaJson;
        
        if (!responseData || !responseData.success) {
          throw new Error("Erro na API do Sankhya: " + JSON.stringify(sankhyaJson));
        }

        const responseBody = responseData.data.responseBody;
        const rows = responseBody.rows || [];
        
        if (rows.length === 0) {
          hasMore = false;
        } else {
          const fields = responseBody.fieldsMetadata.map((f: any) => f.name);

          const dataToUpsert = rows.map((rowArray: any[]) => {
            const obj: Record<string, any> = {};
            fields.forEach((field: string, i: number) => { obj[field] = rowArray[i]; });
            return {
              id: obj.ID, codemp: obj.CODEMP, empresa: obj.EMPRESA, codctabcoint: obj.CODCTABCOINT,
              codctabco: obj.CODCTABCO, banco: obj.BANCO, codcencus: obj.CODCENCUS, centro_custos: obj.CENTRO_CUSTOS,
              codnat: obj.CODNAT, natureza: obj.NATUREZA, codparc: obj.CODPARC, parceiro: (obj.PARCEIRO && String(obj.PARCEIRO).trim()) || parceirosMap.get(Number(obj.CODPARC)) || (obj.CODPARC ? `Parceiro ${obj.CODPARC}` : null),
              valor: obj.VALOR, valor_baixa: obj.VALOR_BAIXA, valor_imposto: obj.VALOR_IMPOSTO, valor_liquido: obj.VALOR_LIQUIDO, data_vencimento: parseSankhyaDate(obj.DATA_VENCIMENTO, false),
              data_competencia: parseSankhyaDate(obj.DATA_COMPETENCIA, false), data_baixa: parseSankhyaDate(obj.DATA_BAIXA, false),
              historico: obj.HISTORICO, status: obj.STATUS, provisionado_real: obj.PROVISIONADO_REAL,
              tipo_lancamento: obj.TIPO_LANCAMENTO, dtalter: parseSankhyaDate(obj.DTALTER, true),
              usuario: obj.USUARIO, usuario_baixa: obj.USUARIO_BAIXA,
              emenda: obj.EMENDA?.toUpperCase() === 'SIM',
              nf: obj.NF || obj.nf
            };
          });

          let insertedCount = 0;
          const chunkSize = 1000;
          for (let i = 0; i < dataToUpsert.length; i += chunkSize) {
            const chunk = dataToUpsert.slice(i, i + chunkSize);
            const { error: upsertError } = await supabase.from('sankhya_lancamentos').upsert(chunk, { onConflict: 'id' });
            if (upsertError) throw new Error(`Erro de Upsert: ${upsertError.message}`);
            insertedCount += chunk.length;
          }
          
          totalInserted += insertedCount;
          lastId = Number(rows[rows.length - 1][0]);
          await writeLog(`Página ${pageCount} processada. ${insertedCount} registros salvos. Total acumulado: ${totalInserted}`);
        }
      }
      
      await writeLog(`Resync Completo Concluído! ${totalInserted} registros inseridos/atualizados.`, 'success');
      return new Response(JSON.stringify({ success: true, message: `Resync Completo Concluído. ${totalInserted} registros processados.` }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }

    let deletedCount = 0;

    // MODO FULL (DELETA ÓRFÃOS E DEPOIS CAI NO INCREMENTAL)
    if (mode === 'full') {
      await writeLog("Buscando todos os registros locais do banco (Supabase)...");
      const { data: localData, error: localError } = await supabase.from('sankhya_lancamentos').select('id');
      if (localError) throw localError;
      
      const localIds = new Set(localData.map(d => String(d.id)));
      await writeLog(`${localIds.size} registros locais encontrados.`);

      await writeLog("Iniciando varredura paginada na API do Sankhya para identificar órfãos...");
      const remoteIds = new Set<string>();
      let hasMore = true;
      let lastId = 0;
      let pageCount = 0;

      while (hasMore) {
        pageCount++;
        await writeLog(`Buscando página ${pageCount} (ID > ${lastId})...`);
        
        const sankhyaQuery = `SELECT ID FROM VW_FINANCEIRO_FULL WHERE DATA_COMPETENCIA >= TO_DATE('2025-01-01', 'YYYY-MM-DD') AND ID > ${lastId} ORDER BY ID ASC`;

        const sankhyaRes = await fetch("https://api-sankhya.pluralmed.com.br/api/query", {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${sankhyaToken}`,
            "ApiKey": sankhyaToken,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            body: { requestBody: { sql: sankhyaQuery }, serviceName: "DbExplorerSP.executeQuery" },
            module: "mge",
            service_name: "string"
          })
        });

        const sankhyaJson = await sankhyaRes.json();
        const responseData = Array.isArray(sankhyaJson) ? sankhyaJson[0] : sankhyaJson;
        
        if (!responseData || !responseData.success) {
          throw new Error("Erro na API do Sankhya: " + JSON.stringify(sankhyaJson));
        }

        const rows = responseData.data.responseBody.rows || [];
        
        if (rows.length === 0) {
          hasMore = false;
        } else {
          rows.forEach((rowArray: any[]) => remoteIds.add(String(rowArray[0])));
          lastId = Number(rows[rows.length - 1][0]);
        }
      }
      
      await writeLog(`Varredura concluída! ${remoteIds.size} registros remotos encontrados em ${pageCount} requisições.`);
      await writeLog("Comparando base local com base do Sankhya...");

      const idsToDelete: string[] = [];
      for (const localId of localIds) {
        if (!remoteIds.has(localId)) {
          idsToDelete.push(localId);
        }
      }

      await writeLog(`Comparação concluída. ${idsToDelete.length} registros serão deletados.`);

      if (idsToDelete.length > 0) {
        const chunkSize = 500;
        for (let i = 0; i < idsToDelete.length; i += chunkSize) {
          const chunk = idsToDelete.slice(i, i + chunkSize);
          const { error: deleteError } = await supabase.from('sankhya_lancamentos').delete().in('id', chunk);
          if (deleteError) throw deleteError;
          deletedCount += chunk.length;
          await writeLog(`Deletando lote de exclusões (${deletedCount} de ${idsToDelete.length})...`);
        }
      }

      await writeLog(`Full Sync (Limpeza): ${deletedCount} registros órfãos excluídos. Iniciando incremental agora...`, 'success');
    }

    // MODO INCREMENTAL
    await writeLog("Buscando a data da última sincronização para o incremental...");
    const { data: latestData, error: latestError } = await supabase.from('sankhya_lancamentos').select('dtalter').order('dtalter', { ascending: false }).limit(1);
    if (latestError) throw latestError;

    let sqlQuery = `SELECT * FROM VW_FINANCEIRO_FULL WHERE 1=1 `;
    if (start_date && end_date) {
      sqlQuery += ` AND DATA_COMPETENCIA >= TO_DATE('${start_date}', 'YYYY-MM-DD') AND DATA_COMPETENCIA < TO_DATE('${end_date}', 'YYYY-MM-DD')`;
      await writeLog(`Modo Histórico: Buscando de ${start_date} até ${end_date}`);
    } else if (latestData && latestData.length > 0 && latestData[0].dtalter) {
      const lastSyncStr = latestData[0].dtalter.substring(0, 19).replace('T', ' ');
      sqlQuery += ` AND DTALTER > TO_DATE('${lastSyncStr}', 'YYYY-MM-DD HH24:MI:SS')`;
      await writeLog(`Modo Incremental: Buscando alterações após ${lastSyncStr}`);
    } else {
      sqlQuery += ` AND DATA_COMPETENCIA >= TO_DATE('2025-01-01', 'YYYY-MM-DD')`;
      await writeLog(`Modo Inicial: Buscando a partir de 2025`);
    }
    sqlQuery += ` ORDER BY DTALTER ASC`;

    await writeLog("Consultando API do Sankhya (Dados incrementais)...");
    const sankhyaRes = await fetch("https://api-sankhya.pluralmed.com.br/api/query", {
      method: "POST",
      headers: { "Authorization": `Bearer ${sankhyaToken}`, "ApiKey": sankhyaToken, "Content-Type": "application/json" },
      body: JSON.stringify({ body: { requestBody: { sql: sqlQuery }, serviceName: "DbExplorerSP.executeQuery" }, module: "mge", service_name: "string" })
    });
    const sankhyaJson = await sankhyaRes.json();
    const responseData = Array.isArray(sankhyaJson) ? sankhyaJson[0] : sankhyaJson;
    
    if (!responseData || !responseData.success) {
      throw new Error("Erro na API do Sankhya: " + JSON.stringify(sankhyaJson));
    }

    const responseBody = responseData.data.responseBody;
    const rows = responseBody.rows || [];
    
    if (rows.length === 0) {
      await writeLog("Nenhum registro novo ou alterado encontrado. Tudo atualizado!", 'success');
      return new Response(JSON.stringify({ 
        success: true, 
        message: mode === 'full' 
          ? `Sincronização Concluída! ${deletedCount} removidos e 0 novos/alterados.` 
          : `0 registros novos para o período.` 
      }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    await writeLog(`${rows.length} registros retornados no incremental. Preparando dados para inserção...`);
    const fields = responseBody.fieldsMetadata.map((f: any) => f.name);

    const dataToUpsert = rows.map((rowArray: any[]) => {
      const obj: Record<string, any> = {};
      fields.forEach((field: string, i: number) => { obj[field] = rowArray[i]; });
      return {
        id: obj.ID, codemp: obj.CODEMP, empresa: obj.EMPRESA, codctabcoint: obj.CODCTABCOINT,
        codctabco: obj.CODCTABCO, banco: obj.BANCO, codcencus: obj.CODCENCUS, centro_custos: obj.CENTRO_CUSTOS,
        codnat: obj.CODNAT, natureza: obj.NATUREZA, codparc: obj.CODPARC, parceiro: (obj.PARCEIRO && String(obj.PARCEIRO).trim()) || parceirosMap.get(Number(obj.CODPARC)) || (obj.CODPARC ? `Parceiro ${obj.CODPARC}` : null),
        valor: obj.VALOR, valor_baixa: obj.VALOR_BAIXA, valor_imposto: obj.VALOR_IMPOSTO, valor_liquido: obj.VALOR_LIQUIDO, data_vencimento: parseSankhyaDate(obj.DATA_VENCIMENTO, false),
        data_competencia: parseSankhyaDate(obj.DATA_COMPETENCIA, false), data_baixa: parseSankhyaDate(obj.DATA_BAIXA, false),
        historico: obj.HISTORICO, status: obj.STATUS, provisionado_real: obj.PROVISIONADO_REAL,
        tipo_lancamento: obj.TIPO_LANCAMENTO, dtalter: parseSankhyaDate(obj.DTALTER, true),
        usuario: obj.USUARIO, usuario_baixa: obj.USUARIO_BAIXA,
        emenda: obj.EMENDA?.toUpperCase() === 'SIM',
        nf: obj.NF || obj.nf
      };
    });

    let insertedCount = 0;
    const chunkSize = 1000;
    for (let i = 0; i < dataToUpsert.length; i += chunkSize) {
      const chunk = dataToUpsert.slice(i, i + chunkSize);
      const { error: upsertError } = await supabase.from('sankhya_lancamentos').upsert(chunk, { onConflict: 'id' });
      if (upsertError) throw new Error(`Erro de Upsert: ${upsertError.message}`);
      insertedCount += chunk.length;
      await writeLog(`Salvando lote de dados (${insertedCount} de ${dataToUpsert.length})...`);
    }

    await writeLog(`Sincronização Incremental Concluída! ${insertedCount} registros atualizados/inseridos.`, 'success');
    
    return new Response(JSON.stringify({ 
      success: true, 
      message: mode === 'full' 
        ? `Varredura Completa: ${deletedCount} registros excluídos e ${dataToUpsert.length} atualizados/inseridos.` 
        : `${dataToUpsert.length} registros inseridos/atualizados.` 
    }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (err: any) {
    console.error(`[FATAL ERROR]`, err);
    try {
      const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
      const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
      const supabase = createClient(supabaseUrl, supabaseKey);
      await supabase.from('sync_logs').insert({ message: `Erro: ${err.message}`, level: 'error', sync_type: 'error' });
    } catch (e) {
      console.error(e);
    }
    
    return new Response(JSON.stringify({ success: false, error: String(err?.message || err) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });
  }
});
