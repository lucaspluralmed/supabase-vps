import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
    const sankhyaToken = Deno.env.get("SANKHYA_API_KEY") ?? "";

    // Sem fallback literal: credencial nunca mora no codigo. Se o secret nao
    // estiver configurado, a function falha alto em vez de rodar com uma
    // chave versionada no git.
    if (!supabaseUrl || !supabaseKey || !sankhyaToken) {
      const faltando = [
        !supabaseUrl ? "SUPABASE_URL" : null,
        !supabaseKey ? "SUPABASE_SERVICE_ROLE_KEY" : null,
        !sankhyaToken ? "SANKHYA_API_KEY" : null,
      ].filter(Boolean).join(", ");
      console.error(`[SYNC_MAP] Secrets ausentes: ${faltando}`);
      return new Response(
        JSON.stringify({ success: false, error: `Configuracao incompleta: ${faltando}` }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    const supabase = createClient(supabaseUrl, supabaseKey);

    console.log("[SYNC_MAP] Iniciando sincronização do catálogo com VW_MAP_FINANCEIRO...");

    // 1. Limpeza segura dos registros não mapeados
    const { count: deletedCount, error: deleteError } = await supabase
      .from('sankhya_mapeamento_dre')
      .delete({ count: 'exact' })
      .is('dre_conta_id', null);

    if (deleteError) {
      console.error("[SYNC_MAP] Erro ao limpar itens não mapeados:", deleteError);
      throw deleteError;
    }
    console.log(`[SYNC_MAP] Removidos ${deletedCount || 0} registros não mapeados.`);

    // 2. Consulta à view VW_MAP_FINANCEIRO na API Sankhya
    const res = await fetch("https://api-sankhya.pluralmed.com.br/api/query", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${sankhyaToken}`,
        "ApiKey": sankhyaToken,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        body: {
          requestBody: {
            sql: "SELECT * FROM VW_MAP_FINANCEIRO"
          },
          serviceName: "DbExplorerSP.executeQuery"
        },
        module: "mge",
        service_name: "string"
      })
    });

    if (!res.ok) {
      throw new Error(`Erro na API Sankhya: status ${res.status}`);
    }

    const json = await res.json();
    const responseData = Array.isArray(json) ? json[0] : json;
    const rows = responseData?.data?.responseBody?.rows || [];
    console.log(`[SYNC_MAP] Obtidas ${rows.length} combinações da view VW_MAP_FINANCEIRO.`);

    if (rows.length === 0) {
      return new Response(JSON.stringify({
        success: true,
        deletedCount: deletedCount || 0,
        insertedCount: 0,
        totalApiRows: 0,
        message: "Nenhuma linha retornada pela view VW_MAP_FINANCEIRO."
      }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }

    // 3. Carrega todos os mapeamentos existentes no banco para comparação
    const { data: existingMaps, error: fetchExistingError } = await supabase
      .from('sankhya_mapeamento_dre')
      .select('codigo_empresa_sankhya, codigo_centro_custo_sankhya, codigo_natureza_sankhya, usa_valor_imposto');

    if (fetchExistingError) {
      console.error("[SYNC_MAP] Erro ao buscar mapeamentos existentes:", fetchExistingError);
      throw fetchExistingError;
    }

    const existingKeys = new Set<string>();
    (existingMaps || []).forEach(m => {
      const cc = m.codigo_centro_custo_sankhya !== null && m.codigo_centro_custo_sankhya !== undefined 
        ? m.codigo_centro_custo_sankhya 
        : -1;
      const key = `${m.codigo_empresa_sankhya}_${cc}_${m.codigo_natureza_sankhya}_${m.usa_valor_imposto ? 'true' : 'false'}`;
      existingKeys.add(key);
    });

    // 4. Filtra combinações da view que ainda não existem no banco
    const toInsert: any[] = [];
    const processedKeys = new Set<string>();

    rows.forEach((r: any[]) => {
      const codEmp = Number(r[0]);
      const nomeEmp = r[1] ? String(r[1]).trim() : null;
      const codCc = r[2] !== null && r[2] !== undefined && !isNaN(Number(r[2])) ? Number(r[2]) : null;
      const nomeCc = r[3] ? String(r[3]).trim() : null;
      const codNat = Number(r[4]);
      const nomeNat = r[5] ? String(r[5]).trim() : null;

      if (!codEmp || !codNat) return;

      const ccLookup = codCc !== null ? codCc : -1;
      const key = `${codEmp}_${ccLookup}_${codNat}_false`;

      if (!existingKeys.has(key) && !processedKeys.has(key)) {
        processedKeys.add(key);
        toInsert.push({
          codigo_empresa_sankhya: codEmp,
          nome_empresa_sankhya: nomeEmp,
          codigo_centro_custo_sankhya: codCc,
          nome_centro_custo_sankhya: nomeCc,
          codigo_natureza_sankhya: codNat,
          nome_natureza_sankhya: nomeNat,
          tipo_regra: 'RECEITA_CUSTO',
          usa_valor_imposto: false,
          dre_empresa_id: null,
          dre_categoria_id: null,
          dre_nivel1_nome: null,
          dre_conta_id: null
        });
      }
    });

    console.log(`[SYNC_MAP] Identificadas ${toInsert.length} novas combinações a inserir.`);

    // 5. Inserção em lotes (chunks) de 200 itens
    let insertedCount = 0;
    const CHUNK_SIZE = 200;
    for (let i = 0; i < toInsert.length; i += CHUNK_SIZE) {
      const chunk = toInsert.slice(i, i + CHUNK_SIZE);
      const { error: insertError } = await supabase
        .from('sankhya_mapeamento_dre')
        .insert(chunk);

      if (insertError) {
        console.error(`[SYNC_MAP] Erro ao inserir lote ${i / CHUNK_SIZE + 1}:`, insertError);
        throw insertError;
      }
      insertedCount += chunk.length;
    }

    console.log(`[SYNC_MAP] Sincronização concluída com sucesso! Inseridos: ${insertedCount}`);

    return new Response(JSON.stringify({
      success: true,
      deletedCount: deletedCount || 0,
      insertedCount,
      totalApiRows: rows.length,
      message: `Sincronização concluída: ${deletedCount || 0} pendentes limpos e ${insertedCount} novas combinações adicionadas.`
    }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

  } catch (err: any) {
    console.error("[SYNC_MAP] Erro geral na execução:", err);
    return new Response(JSON.stringify({
      success: false,
      error: err.message || "Erro desconhecido ao sincronizar catálogo."
    }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });
  }
});
