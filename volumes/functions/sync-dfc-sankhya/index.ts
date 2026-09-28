import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";
import { sha256 } from "https://esm.sh/js-sha256";

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
  
  if (asTimestamp && dateStr.length >= 15) {
    const time = dateStr.substring(9);
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
    const { mode = 'incremental' } = reqBody; // 'resync_all', 'full', 'incremental'

    const sankhyaToken = Deno.env.get("SANKHYA_API_KEY");
    if (!sankhyaToken) {
      throw new Error("SANKHYA_API_KEY não encontrada.");
    }

    const writeLog = async (message: string, level: string = 'info') => {
      console.log(`[${level.toUpperCase()}] ${message}`);
      await supabase.from('sync_logs').insert({ message, level, sync_type: `dfc_${mode}` });
    };

    async function executeQuery(query: string) {
      let retries = 5;
      while (retries > 0) {
        try {
          const sankhyaRes = await fetch("https://api-sankhya.pluralmed.com.br/api/query", {
            method: "POST",
            headers: {
              "Authorization": `Bearer ${sankhyaToken}`,
              "ApiKey": sankhyaToken,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              body: { requestBody: { sql: query }, serviceName: "DbExplorerSP.executeQuery" },
              module: "mge",
              service_name: "string"
            })
          });

          const rawText = await sankhyaRes.text();
          let json: any = null;
          try {
            json = JSON.parse(rawText);
          } catch (e) {
            throw new Error(`Erro ao converter JSON: ${rawText}`);
          }
          
          if (!sankhyaRes.ok) throw new Error(`HTTP ${sankhyaRes.status}: ${rawText}`);
          
          const responseData = Array.isArray(json) ? json[0] : json;
          if (!responseData || !responseData.success) {
            throw new Error(`Sankhya Error: ${JSON.stringify(json)}`);
          }
          
          return responseData.data.responseBody;
        } catch (err: any) {
          retries--;
          await writeLog(`Falha na requisição. Erro: ${err.message}. Tentativas restantes: ${retries}`, 'warn');
          if (retries === 0) throw new Error(`Máximo de tentativas excedido.`);
          await new Promise(r => setTimeout(r, 10000));
        }
      }
    }

    await writeLog(`Iniciando sincronização DFC Sankhya (Modo: ${mode})...`, 'info');

    let totalInserted = 0;
    let totalDeleted = 0;

    // MODO RESYNC_ALL
    if (mode === 'resync_all') {
      await writeLog("Iniciando Resync Completo...");
      
      let hasMore = true;
      let offset = 0;
      const limit = 5000;

      while (hasMore) {
        await writeLog(`Buscando Sankhya (Offset: ${offset}, Limit: ${limit})...`);
        const query = `
          SELECT * FROM (
            SELECT a.*, ROWNUM rnum FROM (\n              SELECT * FROM VW_DFC_CONSOLIDADA_FINAL 
              WHERE DATA >= TO_DATE('2025-01-01', 'YYYY-MM-DD')
              ORDER BY DATA DESC, CODEMP, COD_HIST
            ) a WHERE ROWNUM <= ${offset + limit}
          ) WHERE rnum > ${offset}
        `;
        
        const responseBody = await executeQuery(query);
        const rows = responseBody.rows || [];
        
        if (rows.length === 0) {
          hasMore = false;
        } else {
          const fields = responseBody.fieldsMetadata.map((f: any) => f.name);
          const dataToUpsert = [];
          
          const dataIdx = fields.indexOf("DATA");
          const codempIdx = fields.indexOf("CODEMP");
          const empresaIdx = fields.indexOf("EMPRESA");
          const codContaIdx = fields.indexOf("COD_CONTA_BANCARIA");
          const contaBancariaIdx = fields.indexOf("CONTA_BANCARIA");
          const grupoDfcIdx = fields.indexOf("GRUPO_DFC");
          const nivel1Idx = fields.indexOf("NIVEL_1");
          const nivel2Idx = fields.indexOf("NIVEL_2");
          const nivel3Idx = fields.indexOf("NIVEL_3");
          const nivel4Idx = fields.indexOf("NIVEL_4");
          const naturezaIdx = fields.indexOf("NATUREZA");
          const parceiroIdx = fields.indexOf("PARCEIRO");
          const nfIdx = fields.indexOf("NF");
          const historicoIdx = fields.indexOf("COD_HIST");
          const valorIdx = fields.indexOf("VALOR");
          const dtalterIdx = fields.indexOf("DTALTER");

          for (let r = 0; r < rows.length; r++) {
            const rowArray = rows[r];
            const rawDate = rowArray[dataIdx];
            const parsedDate = parseSankhyaDate(rawDate);
            const parsedDtAlter = parseSankhyaDate(rowArray[dtalterIdx], true);
            const hashStr = `${parsedDate}_${rowArray[codempIdx]}_${rowArray[codContaIdx]}_${rowArray[historicoIdx]}_${rowArray[valorIdx]}`;
            const hash_id = sha256(hashStr);

            dataToUpsert.push({
              hash_id: hash_id,
              data: parsedDate,
              codigo_empresa: rowArray[codempIdx],
              empresa_nome: rowArray[empresaIdx],
              codigo_conta_bancaria: rowArray[codContaIdx],
              conta_bancaria_nome: rowArray[contaBancariaIdx],
              grupo_dfc: rowArray[grupoDfcIdx],
              nivel_1: rowArray[nivel1Idx],
              nivel_2: rowArray[nivel2Idx],
              nivel_3: rowArray[nivel3Idx],
              nivel_4: rowArray[nivel4Idx],
              natureza_nome: rowArray[naturezaIdx],
              parceiro_nome: rowArray[parceiroIdx],
              numero_nota: rowArray[nfIdx] ? String(rowArray[nfIdx]) : null,
              historico: rowArray[historicoIdx],
              valor: rowArray[valorIdx],
              dtalter: parsedDtAlter
            });
          }

          const chunkSize = 1000;
          for (let i = 0; i < dataToUpsert.length; i += chunkSize) {
            const chunk = dataToUpsert.slice(i, i + chunkSize);
            const { error: upsertError } = await supabase.from('sankhya_dfc_consolidada').upsert(chunk, { onConflict: 'hash_id' });
            if (upsertError) throw new Error(`Erro Upsert: ${upsertError.message}`);
          }
          
          totalInserted += dataToUpsert.length;
          offset += limit;
        }
      }
      
      await writeLog(`Resync Completo Concluído! ${totalInserted} registros upsertados.`, 'success');
      return new Response(JSON.stringify({ success: true, message: `Resync Completo: ${totalInserted} upserts.` }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }

    // MODO FULL
    if (mode === 'full') {
      await writeLog("Modo FULL: Identificando órfãos locais...");
      
      const cutOffDate = new Date();
      cutOffDate.setDate(cutOffDate.getDate() - 180);
      const cutOffStr = cutOffDate.toISOString().substring(0, 10);
      
      await writeLog(`Buscando registros locais após ${cutOffStr}...`);
      const { data: localData, error: localError } = await supabase
        .from('sankhya_dfc_consolidada')
        .select('hash_id')
        .gte('data', cutOffStr);
        
      if (localError) throw localError;

      const localIds = new Set((localData || []).map(d => d.hash_id));
      const remoteIds = new Set<string>();
      
      let hasMore = true;
      let offset = 0;
      const limit = 5000;

      while (hasMore) {
        await writeLog(`Buscando Órfãos no Sankhya (Offset: ${offset})...`);
        const query = `
          SELECT * FROM (
            SELECT a.*, ROWNUM rnum FROM (\n              SELECT DATA, CODEMP, COD_CONTA_BANCARIA, COD_HIST, VALOR 
              FROM VW_DFC_CONSOLIDADA_FINAL 
              WHERE DATA >= TO_DATE('${cutOffStr}', 'YYYY-MM-DD')
              ORDER BY DATA DESC, CODEMP, COD_HIST
            ) a WHERE ROWNUM <= ${offset + limit}
          ) WHERE rnum > ${offset}
        `;
        
        try {
           const responseBody = await executeQuery(query);
           const rows = responseBody.rows || [];
           if (rows.length === 0) {
             hasMore = false;
           } else {
             const fields = responseBody.fieldsMetadata.map((f: any) => f.name);
             const dataIdx = fields.indexOf("DATA");
             const codempIdx = fields.indexOf("CODEMP");
             const codContaIdx = fields.indexOf("COD_CONTA_BANCARIA");
             const codHistIdx = fields.indexOf("COD_HIST");
             const valorIdx = fields.indexOf("VALOR");

             for (let r = 0; r < rows.length; r++) {
                const rowArray = rows[r];
                const rawDate = rowArray[dataIdx];
                const codemp = rowArray[codempIdx];
                const codConta = rowArray[codContaIdx];
                const codHist = rowArray[codHistIdx];
                const valor = rowArray[valorIdx];

                const parsedDate = parseSankhyaDate(rawDate);
                const hashStr = `${parsedDate}_${codemp}_${codConta}_${codHist}_${valor}`;
                remoteIds.add(sha256(hashStr));
             }
             offset += limit;
           }
        } catch (e) {
           hasMore = false;
           await writeLog(`Erro na busca otimizada de órfãos. Pulemos e faremos incremental...`, 'warn');
        }
      }

      if (remoteIds.size > 0) {
        const idsToDelete: string[] = [];
        for (const localId of localIds) {
          if (!remoteIds.has(localId)) {
            idsToDelete.push(localId);
          }
        }

        if (idsToDelete.length > 0) {
          await writeLog(`Comparação concluída. ${idsToDelete.length} registros serão deletados.`);
          const chunkSize = 500;
          for (let i = 0; i < idsToDelete.length; i += chunkSize) {
            const chunk = idsToDelete.slice(i, i + chunkSize);
            const { error: deleteError } = await supabase.from('sankhya_dfc_consolidada').delete().in('hash_id', chunk);
            if (deleteError) throw deleteError;
            totalDeleted += chunk.length;
          }
        }
      }
      
      await writeLog(`Full Sync (Limpeza) Concluída. ${totalDeleted} removidos. Caindo pro incremental agora...`);
    }

    // MODO INCREMENTAL
    await writeLog("Buscando a data da última sincronização para o incremental...");
    const { data: latestData, error: latestError } = await supabase.from('sankhya_dfc_consolidada').select('dtalter').order('dtalter', { ascending: false }).limit(1);
    if (latestError) throw latestError;

    let baseQuery = `SELECT * FROM VW_DFC_CONSOLIDADA_FINAL WHERE DATA >= TO_DATE('2025-01-01', 'YYYY-MM-DD')`;
    if (latestData && latestData.length > 0 && latestData[0].dtalter) {
      const lastSyncStr = latestData[0].dtalter.substring(0, 19).replace('T', ' ');
      baseQuery += ` AND DTALTER > TO_DATE('${lastSyncStr}', 'YYYY-MM-DD HH24:MI:SS')`;
      await writeLog(`Modo Incremental: Buscando alterações após ${lastSyncStr}`);
    } else {
      await writeLog(`Nenhum dtalter salvo. Buscando histórico completo.`);
    }

    let hasMoreIncremental = true;
    let offsetIncremental = 0;
    const limitIncremental = 5000;

    while (hasMoreIncremental) {
      await writeLog(`Buscando Incremental (Offset: ${offsetIncremental})...`);
      
      const query = `
        SELECT * FROM (
          SELECT a.*, ROWNUM rnum FROM (\n            ${baseQuery} ORDER BY DTALTER ASC, DATA DESC, CODEMP, COD_HIST
          ) a WHERE ROWNUM <= ${offsetIncremental + limitIncremental}
        ) WHERE rnum > ${offsetIncremental}
      `;
      
      const responseBody = await executeQuery(query);
      const rows = responseBody.rows || [];
      
      if (rows.length === 0) {
        hasMoreIncremental = false;
      } else {
        const fields = responseBody.fieldsMetadata.map((f: any) => f.name);
        const dataToUpsert = [];
        
        const dataIdx = fields.indexOf("DATA");
        const codempIdx = fields.indexOf("CODEMP");
        const empresaIdx = fields.indexOf("EMPRESA");
        const codContaIdx = fields.indexOf("COD_CONTA_BANCARIA");
        const contaBancariaIdx = fields.indexOf("CONTA_BANCARIA");
        const grupoDfcIdx = fields.indexOf("GRUPO_DFC");
        const nivel1Idx = fields.indexOf("NIVEL_1");
        const nivel2Idx = fields.indexOf("NIVEL_2");
        const nivel3Idx = fields.indexOf("NIVEL_3");
        const nivel4Idx = fields.indexOf("NIVEL_4");
        const naturezaIdx = fields.indexOf("NATUREZA");
        const parceiroIdx = fields.indexOf("PARCEIRO");
        const nfIdx = fields.indexOf("NF");
        const historicoIdx = fields.indexOf("COD_HIST");
        const valorIdx = fields.indexOf("VALOR");
        const dtalterIdx = fields.indexOf("DTALTER");

        for (let r = 0; r < rows.length; r++) {
          const rowArray = rows[r];
          const rawDate = rowArray[dataIdx];
          const parsedDate = parseSankhyaDate(rawDate);
          const parsedDtAlter = parseSankhyaDate(rowArray[dtalterIdx], true);
          const hashStr = `${parsedDate}_${rowArray[codempIdx]}_${rowArray[codContaIdx]}_${rowArray[historicoIdx]}_${rowArray[valorIdx]}`;
          const hash_id = sha256(hashStr);

          dataToUpsert.push({
            hash_id: hash_id,
            data: parsedDate,
            codigo_empresa: rowArray[codempIdx],
            empresa_nome: rowArray[empresaIdx],
            codigo_conta_bancaria: rowArray[codContaIdx],
            conta_bancaria_nome: rowArray[contaBancariaIdx],
            grupo_dfc: rowArray[grupoDfcIdx],
            nivel_1: rowArray[nivel1Idx],
            nivel_2: rowArray[nivel2Idx],
            nivel_3: rowArray[nivel3Idx],
            nivel_4: rowArray[nivel4Idx],
            natureza_nome: rowArray[naturezaIdx],
            parceiro_nome: rowArray[parceiroIdx],
            numero_nota: rowArray[nfIdx] ? String(rowArray[nfIdx]) : null,
            historico: rowArray[historicoIdx],
            valor: rowArray[valorIdx],
            dtalter: parsedDtAlter
          });
        }

        const chunkSize = 1000;
        for (let i = 0; i < dataToUpsert.length; i += chunkSize) {
          const chunk = dataToUpsert.slice(i, i + chunkSize);
          const { error: upsertError } = await supabase.from('sankhya_dfc_consolidada').upsert(chunk, { onConflict: 'hash_id' });
          if (upsertError) throw new Error(`Erro Upsert: ${upsertError.message}`);
        }
        
        totalInserted += dataToUpsert.length;
        offsetIncremental += limitIncremental;
      }
    }

    if (totalInserted === 0 && totalDeleted === 0) {
      await writeLog("Nenhum registro novo ou alterado encontrado. Tudo atualizado!", 'success');
    } else {
      await writeLog(`Sincronização Incremental Concluída! ${totalInserted} atualizados/inseridos.`, 'success');
    }
    
    return new Response(JSON.stringify({ 
      success: true, 
      message: mode === 'full' 
        ? `Sincronização Full/Incremental Concluída! ${totalDeleted} removidos and ${totalInserted} inseridos/atualizados.` 
        : `Sincronização Incremental Concluída! ${totalInserted} registros atualizados.` 
    }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (err: any) {
    console.error(`[FATAL ERROR]`, err);
    try {
      const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
      const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
      const supabase = createClient(supabaseUrl, supabaseKey);
      await supabase.from('sync_logs').insert({ message: `Erro DFC: ${err.message}`, level: 'error', sync_type: 'error' });
    } catch (e) {
      console.error(e);
    }
    
    return new Response(JSON.stringify({ success: false, error: String(err?.message || err) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });
  }
});
