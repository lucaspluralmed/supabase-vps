import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

// ==============================================================================
// SYNC SALDO BANCÁRIO
//
// Espelha a VW_SALDO_BANCARIO do Sankhya (TGFSBC + TSICTA) na tabela
// sankhya_saldos_bancarios. Uma linha por conta bancária e mês de referência (dia 1);
// saldo_real é o saldo com que aquele mês ABRE, isto é, o fechamento do mês anterior
// (a referência 01/02 guarda o fechamento de janeiro). Usada pela Auditoria de Conciliação
// para comparar o saldo do Sankhya com o saldo do extrato informado pelo usuário.
//
// Carga completa a cada execução (a view tem poucas centenas de linhas):
//   1. baixa tudo do Sankhya (paginado, com retentativas);
//   2. upsert por (codctabcoint, referencia) marcando synced_at = início da execução;
//   3. apaga o que não veio (synced_at anterior ao início). Só apaga se algo foi recebido.
// ==============================================================================

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

const PAGE_SIZE = 1000;
const CHUNK = 500;
const MAX_TENTATIVAS = 5;
const ORCAMENTO_MS = 120_000;

const SQL_SALDOS = `
  SELECT v.CODCTABCOINT, v.CODCTABCO, v.DESCRICAO, c.CODEMP, c.CLASSE, c.ATIVA,
         v.REFERENCIA, v.SALDOREAL, s.SALDOBCO
  FROM VW_SALDO_BANCARIO v
  LEFT JOIN TSICTA c ON c.CODCTABCOINT = v.CODCTABCOINT
  LEFT JOIN TGFSBC s ON s.CODCTABCOINT = v.CODCTABCOINT AND s.REFERENCIA = v.REFERENCIA
  ORDER BY v.CODCTABCOINT, v.REFERENCIA
`;

// "01092026 00:00:00" -> "2026-09-01"
function parseSankhyaDate(dateStr: string | null | undefined): string | null {
  if (!dateStr || String(dateStr).trim() === '') return null;
  const s = String(dateStr);
  return `${s.substring(4, 8)}-${s.substring(2, 4)}-${s.substring(0, 2)}`;
}

const sleep = (ms: number) => new Promise(res => setTimeout(res, ms));

serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  const inicio = Date.now();
  const inicioIso = new Date(inicio).toISOString();
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
  const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
  const supabase = createClient(supabaseUrl, supabaseKey);

  const writeLog = async (message: string, level: string = 'info') => {
    console.log(`[${level.toUpperCase()}] ${message}`);
    await supabase.from('sync_logs').insert({ message, level, sync_type: 'saldo_bancario' });
  };

  try {
    // ---------------- Autorização: mesmo gate da sync-financeiro-unificado ----------------
    const authHeader = req.headers.get('Authorization');
    const cronSecret = Deno.env.get("CRON_SECRET") ?? "";

    let isAuthorized = false;
    if (cronSecret && authHeader === `Bearer ${cronSecret}`) {
      isAuthorized = true;
    } else if (authHeader) {
      const jwt = authHeader.replace('Bearer ', '');
      const { data: { user } } = await supabase.auth.getUser(jwt);
      if (user) {
        const { data: perfil } = await supabase
          .from('profiles')
          .select('role, status, telas_acesso')
          .eq('id', user.id)
          .single();
        // Admin ativo, quem tem a tela de sincronização ou quem tem a auditoria financeira
        isAuthorized = !!perfil
          && perfil.status === 'active'
          && (perfil.role === 'admin'
            || (perfil.telas_acesso ?? []).includes('sincronizacao')
            || (perfil.telas_acesso ?? []).includes('auditoria_financeira'));
      }
    }

    if (!isAuthorized) {
      return new Response(JSON.stringify({ error: "Não autorizado." }), {
        status: 401,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }

    const sankhyaToken = Deno.env.get("SANKHYA_API_KEY");
    if (!sankhyaToken) throw new Error("SANKHYA_API_KEY não encontrada.");

    // ---------------- Consulta ao Sankhya com retentativas ----------------
    async function executeQuery(query: string): Promise<any> {
      let ultimoErro = '';
      for (let tentativa = 1; tentativa <= MAX_TENTATIVAS; tentativa++) {
        if (Date.now() - inicio > ORCAMENTO_MS) {
          throw new Error(`Tempo limite atingido antes de concluir a carga. Nada foi alterado.`);
        }
        try {
          const res = await fetch("https://api-sankhya.pluralmed.com.br/api/query", {
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
          const rawText = await res.text();
          if (!res.ok) throw new Error(`HTTP ${res.status}: ${rawText.substring(0, 200)}`);
          let json: any;
          try { json = JSON.parse(rawText); }
          catch (_) { throw new Error(`Resposta não é JSON: ${rawText.substring(0, 200)}`); }
          const responseData = Array.isArray(json) ? json[0] : json;
          if (!responseData || !responseData.success) {
            throw new Error("Erro na API do Sankhya: " + JSON.stringify(json).substring(0, 200));
          }
          return responseData.data?.responseBody;
        } catch (err: any) {
          ultimoErro = err?.message || String(err);
          if (tentativa === MAX_TENTATIVAS) break;
          const espera = Math.min(2000 * Math.pow(2, tentativa - 1), 20_000);
          await writeLog(`Tentativa ${tentativa}/${MAX_TENTATIVAS} falhou (${ultimoErro}). Aguardando ${Math.round(espera / 1000)}s...`, 'warning');
          await sleep(espera);
        }
      }
      throw new Error(`API do Sankhya falhou após ${MAX_TENTATIVAS} tentativas: ${ultimoErro}`);
    }

    await writeLog('Saldos bancários: baixando VW_SALDO_BANCARIO do Sankhya...');

    // ---------------- Baixar tudo ----------------
    const registros: Record<string, any>[] = [];
    let offset = 0;
    for (;;) {
      const sql = `${SQL_SALDOS} OFFSET ${offset} ROWS FETCH NEXT ${PAGE_SIZE} ROWS ONLY`;
      const body = await executeQuery(sql);
      const rows: any[] = body?.rows || [];
      if (rows.length === 0) break;
      const fields: string[] = body.fieldsMetadata.map((f: any) => f.name);
      for (const r of rows) {
        const obj: Record<string, any> = {};
        fields.forEach((f, i) => { obj[f] = r[i]; });
        const referencia = parseSankhyaDate(obj.REFERENCIA);
        if (obj.CODCTABCOINT === null || obj.CODCTABCOINT === undefined || !referencia) continue;
        registros.push({
          codctabcoint: Number(obj.CODCTABCOINT),
          referencia,
          codctabco: obj.CODCTABCO ? String(obj.CODCTABCO) : null,
          descricao: obj.DESCRICAO ? String(obj.DESCRICAO) : null,
          codemp: obj.CODEMP !== null && obj.CODEMP !== undefined ? Number(obj.CODEMP) : null,
          classe_conta: obj.CLASSE ? String(obj.CLASSE) : null,
          ativa: obj.ATIVA === null || obj.ATIVA === undefined ? null : String(obj.ATIVA).toUpperCase() === 'S',
          saldo_real: Number(obj.SALDOREAL || 0),
          saldo_bco: obj.SALDOBCO !== null && obj.SALDOBCO !== undefined ? Number(obj.SALDOBCO) : null,
          synced_at: inicioIso,
        });
      }
      offset += rows.length;
      if (rows.length < PAGE_SIZE) break;
    }

    if (registros.length === 0) {
      throw new Error('O Sankhya não retornou nenhum saldo bancário. Nada foi alterado.');
    }

    // Dedup por chave (a view pode repetir a mesma conta/referência)
    const porChave = new Map<string, Record<string, any>>();
    registros.forEach(r => porChave.set(`${r.codctabcoint}|${r.referencia}`, r));
    const unicos = Array.from(porChave.values());

    // ---------------- Gravar ----------------
    for (let i = 0; i < unicos.length; i += CHUNK) {
      const { error } = await supabase
        .from('sankhya_saldos_bancarios')
        .upsert(unicos.slice(i, i + CHUNK), { onConflict: 'codctabcoint,referencia' });
      if (error) throw new Error(`Erro ao gravar saldos: ${error.message}`);
    }

    // Remove o que não veio mais do Sankhya
    const { data: removidos, error: delErr } = await supabase
      .from('sankhya_saldos_bancarios')
      .delete()
      .lt('synced_at', inicioIso)
      .select('codctabcoint');
    if (delErr) throw new Error(`Erro ao remover saldos antigos: ${delErr.message}`);

    const segundos = Math.round((Date.now() - inicio) / 1000);
    const contas = new Set(unicos.map(r => r.codctabcoint)).size;
    await writeLog(
      `Saldos bancários atualizados em ${segundos}s: ${unicos.length} linhas de ${contas} contas, ${removidos?.length ?? 0} removidas.`,
      'success'
    );

    return new Response(JSON.stringify({
      success: true,
      linhas: unicos.length,
      contas,
      removidos: removidos?.length ?? 0,
      segundos
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (error: any) {
    const msg = error?.message || String(error);
    console.error("Erro na Edge Function sync-saldo-bancario:", error);
    try { await writeLog(`Saldos bancários: erro: ${msg}`, 'error'); } catch (_) { /* ignore */ }
    return new Response(JSON.stringify({ success: false, error: msg }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });
  }
});
