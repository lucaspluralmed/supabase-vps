import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

// ==============================================================================
// SYNC CICLO DOS CONTRATOS
//
// Espelha o CADASTRO dos contratos do Sankhya nas tabelas sankhya_contratos*:
//   TCSCON -> sankhya_contratos | AD_TCSCONCENCUS -> sankhya_contratos_cencus
//   AD_TCSCONCENCUSRUB -> sankhya_contratos_rubricas | AD_TCSCONCENCUSRUBNAT -> ..._rubricas_naturezas
//   AD_TCSCONCENCUSRUBPLA -> sankhya_contratos_planejado | AD_RUBRICA -> sankhya_rubricas
// Os títulos já estão em sankhya_financeiro_unificado; a regra que liga título a contrato roda no
// Postgres (view ciclo_contratos_titulos). Mesma lógica do sync-ciclo-contratos-local.js:
// carga completa, upsert por chave natural e remoção do que não veio (synced_at anterior ao início).
// Baixa as 6 tabelas antes de gravar e aborta sem alterar nada se alguma vier vazia ou com menos
// da metade das linhas atuais (mesma proteção da sync-financeiro-unificado).
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
// Se o Sankhya devolver menos da metade das linhas que já temos numa tabela, a API retornou
// parcial. Abortamos antes de gravar qualquer tabela em vez de apagar cadastro bom.
const PROPORCAO_MINIMA_REMOTA = 0.5;

const SQL_CONTRATOS = `
  SELECT c.NUMCONTRATO, c.CODEMP, e.NOMEFANTASIA AS EMPRESA, c.CODPARC, p.NOMEPARC AS PARCEIRO,
         c.CODCENCUS, cr.DESCRCENCUS AS CENTRO_CUSTOS, c.CODNAT, n.DESCRNAT AS NATUREZA,
         c.RECDESP, c.DTCONTRATO, c.DTTERMINO, c.ATIVO, c.NUMCONTRATOORIGEM, c.AD_NUMCONTRATO,
         c.EQUIPAMENTO AS IDENTIFICADOR, c.AD_OBJETOCONTRATO AS OBJETO,
         c.AD_DTINICOMPETENCIAFATURAMENTO AS DT_INI_COMP_FAT, c.AD_DTFIMCOMPETENCIAFATURAMENTO AS DT_FIM_COMP_FAT,
         c.AD_DTALTER AS DTALTER
    FROM TCSCON c
    LEFT JOIN TSIEMP e  ON e.CODEMP = c.CODEMP
    LEFT JOIN TGFPAR p  ON p.CODPARC = c.CODPARC
    LEFT JOIN TSICUS cr ON cr.CODCENCUS = c.CODCENCUS
    LEFT JOIN TGFNAT n  ON n.CODNAT = c.CODNAT
   ORDER BY c.NUMCONTRATO
`;
const SQL_CENCUS = `
  SELECT cc.NUMCONTRATO, cc.CODTCSCONCENCUS, cc.CODCENCUS, cr.DESCRCENCUS AS CENTRO_CUSTOS
    FROM AD_TCSCONCENCUS cc
    LEFT JOIN TSICUS cr ON cr.CODCENCUS = cc.CODCENCUS
   ORDER BY cc.NUMCONTRATO, cc.CODTCSCONCENCUS
`;
const SQL_RUBRICAS = `
  SELECT NUMCONTRATO, CODTCSCONCENCUS, CODTCSCONCENCUSRUB, CODRUBRICA, VLRCONTRATADO
    FROM AD_TCSCONCENCUSRUB
   ORDER BY NUMCONTRATO, CODTCSCONCENCUS, CODTCSCONCENCUSRUB
`;
const SQL_RUBRICAS_NATUREZAS = `
  SELECT NUMCONTRATO, CODTCSCONCENCUS, CODTCSCONCENCUSRUB, CODTCSCONCENCUSRUBNAT, CODNAT, EXTRAORC
    FROM AD_TCSCONCENCUSRUBNAT
   ORDER BY NUMCONTRATO, CODTCSCONCENCUS, CODTCSCONCENCUSRUB, CODTCSCONCENCUSRUBNAT
`;
const SQL_PLANEJADO = `
  SELECT NUMCONTRATO, CODTCSCONCENCUS, CODTCSCONCENCUSRUB, CODTCSCONCENCUSRUBPLA, DTREF, VLRPLANEJADO, DTCADASTRO
    FROM AD_TCSCONCENCUSRUBPLA
   ORDER BY NUMCONTRATO, CODTCSCONCENCUS, CODTCSCONCENCUSRUB, CODTCSCONCENCUSRUBPLA
`;
const SQL_AD_RUBRICA = `
  SELECT CODRUBRICA, NOME, DESCRITIVO, F_DESCROPC('AD_RUBRICA', 'DESCRITIVO', DESCRITIVO) AS CONTA
    FROM AD_RUBRICA
   ORDER BY CODRUBRICA
`;

// "01092026 00:00:00" -> "2026-09-01"
function parseSankhyaDate(dateStr: unknown): string | null {
  if (!dateStr || String(dateStr).trim() === '') return null;
  const s = String(dateStr);
  return `${s.substring(4, 8)}-${s.substring(2, 4)}-${s.substring(0, 2)}`;
}
function parseSankhyaTimestamp(dateStr: unknown): string | null {
  const d = parseSankhyaDate(dateStr);
  if (!d) return null;
  const s = String(dateStr);
  return s.length >= 17 ? `${d}T${s.substring(9, 17)}` : d;
}
const num = (v: unknown): number | null => (v === null || v === undefined || v === '' ? null : Number(v));
const txt = (v: unknown): string | null => (v === null || v === undefined || String(v).trim() === '' ? null : String(v).trim());
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
    await supabase.from('sync_logs').insert({ message, level, sync_type: 'ciclo_contratos' });
  };

  try {
    // ---------------- Autorização: mesmo gate das demais syncs ----------------
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

    const sankhyaToken = Deno.env.get("SANKHYA_API_KEY");
    if (!sankhyaToken) throw new Error("SANKHYA_API_KEY não encontrada.");

    // ---------------- Consulta ao Sankhya com retentativas ----------------
    async function executeQuery(query: string): Promise<any> {
      let ultimoErro = '';
      for (let tentativa = 1; tentativa <= MAX_TENTATIVAS; tentativa++) {
        if (Date.now() - inicio > ORCAMENTO_MS) {
          throw new Error(`Tempo limite atingido antes de concluir a carga.`);
        }
        try {
          const res = await fetch("https://api-sankhya.pluralmed.com.br/api/query", {
            method: "POST",
            headers: { "Authorization": `Bearer ${sankhyaToken}`, "ApiKey": sankhyaToken, "Content-Type": "application/json" },
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

    async function baixarTudo(sql: string): Promise<Record<string, any>[]> {
      const registros: Record<string, any>[] = [];
      let offset = 0;
      for (;;) {
        const body = await executeQuery(`${sql} OFFSET ${offset} ROWS FETCH NEXT ${PAGE_SIZE} ROWS ONLY`);
        const rows: any[] = body?.rows || [];
        if (rows.length === 0) break;
        const fields: string[] = body.fieldsMetadata.map((f: any) => f.name);
        for (const r of rows) {
          const obj: Record<string, any> = {};
          fields.forEach((f, i) => { obj[f] = r[i]; });
          registros.push(obj);
        }
        offset += rows.length;
        if (rows.length < PAGE_SIZE) break;
      }
      return registros;
    }

    function unicos<T>(linhas: T[], chave: (l: T) => string): T[] {
      const mapa = new Map<string, T>();
      linhas.forEach(l => mapa.set(chave(l), l));
      return Array.from(mapa.values());
    }

    async function validarVolume(tabela: string, recebidas: number) {
      if (recebidas === 0) throw new Error(`O Sankhya não retornou nenhuma linha para ${tabela}. Nada foi alterado.`);
      const { count, error } = await supabase.from(tabela).select('*', { count: 'exact', head: true });
      if (error) throw new Error(`Erro ao contar ${tabela}: ${error.message}`);
      const minimoEsperado = Math.floor((count ?? 0) * PROPORCAO_MINIMA_REMOTA);
      if (recebidas < minimoEsperado) {
        throw new Error(`Retorno suspeito da API em ${tabela}: ${recebidas} linhas recebidas contra ${count} existentes (mínimo aceito: ${minimoEsperado}). Nada foi alterado.`);
      }
    }

    async function gravarTabela(tabela: string, linhas: Record<string, any>[], onConflict: string, colunaSelect: string) {
      for (let i = 0; i < linhas.length; i += CHUNK) {
        const { error } = await supabase.from(tabela).upsert(linhas.slice(i, i + CHUNK), { onConflict });
        if (error) throw new Error(`Erro ao gravar ${tabela}: ${error.message}`);
      }
      const { data: removidos, error: delErr } = await supabase.from(tabela).delete().lt('synced_at', inicioIso).select(colunaSelect);
      if (delErr) throw new Error(`Erro ao remover linhas antigas de ${tabela}: ${delErr.message}`);
      return removidos?.length ?? 0;
    }

    // Toda execução (manual ou cron) começa com a tabela de logs zerada: o modal mostra só a carga atual
    {
      const { error: limpaLogs } = await supabase.from('sync_logs').delete().gte('created_at', '1970-01-01');
      if (limpaLogs) console.warn(`[WARN] Não foi possível limpar sync_logs: ${limpaLogs.message}`);
    }
    await writeLog('Ciclo dos contratos: baixando cadastro de contratos do Sankhya...');

    // ---- TCSCON ----
    const contratos = unicos(
      (await baixarTudo(SQL_CONTRATOS)).filter(o => num(o.NUMCONTRATO) !== null).map(o => ({
        numcontrato: num(o.NUMCONTRATO),
        codemp: num(o.CODEMP),
        empresa: txt(o.EMPRESA),
        codparc: num(o.CODPARC),
        parceiro: txt(o.PARCEIRO),
        codcencus: num(o.CODCENCUS),
        centro_custos: txt(o.CENTRO_CUSTOS),
        codnat: num(o.CODNAT),
        natureza: txt(o.NATUREZA),
        recdesp: num(o.RECDESP),
        dtcontrato: parseSankhyaDate(o.DTCONTRATO),
        dttermino: parseSankhyaDate(o.DTTERMINO),
        ativo: String(o.ATIVO || '').toUpperCase() === 'S',
        numcontrato_origem: num(o.NUMCONTRATOORIGEM),
        ad_numcontrato: num(o.AD_NUMCONTRATO),
        identificador: txt(o.IDENTIFICADOR),
        objeto: txt(o.OBJETO),
        dt_ini_comp_fat: parseSankhyaDate(o.DT_INI_COMP_FAT),
        dt_fim_comp_fat: parseSankhyaDate(o.DT_FIM_COMP_FAT),
        dtalter: parseSankhyaTimestamp(o.DTALTER),
        synced_at: inicioIso,
      })),
      l => String(l.numcontrato)
    );

    // ---- AD_TCSCONCENCUS ----
    const cencus = unicos(
      (await baixarTudo(SQL_CENCUS)).filter(o => num(o.NUMCONTRATO) !== null && num(o.CODTCSCONCENCUS) !== null).map(o => ({
        numcontrato: num(o.NUMCONTRATO),
        codtcsconcencus: num(o.CODTCSCONCENCUS),
        codcencus: num(o.CODCENCUS),
        centro_custos: txt(o.CENTRO_CUSTOS),
        synced_at: inicioIso,
      })),
      l => `${l.numcontrato}|${l.codtcsconcencus}`
    );

    // ---- AD_TCSCONCENCUSRUB ----
    const rubricas = unicos(
      (await baixarTudo(SQL_RUBRICAS)).filter(o => num(o.NUMCONTRATO) !== null && num(o.CODTCSCONCENCUS) !== null && num(o.CODTCSCONCENCUSRUB) !== null).map(o => ({
        numcontrato: num(o.NUMCONTRATO),
        codtcsconcencus: num(o.CODTCSCONCENCUS),
        codtcsconcencusrub: num(o.CODTCSCONCENCUSRUB),
        codrubrica: num(o.CODRUBRICA),
        vlrcontratado: Number(o.VLRCONTRATADO || 0),
        synced_at: inicioIso,
      })),
      l => `${l.numcontrato}|${l.codtcsconcencus}|${l.codtcsconcencusrub}`
    );

    // ---- AD_TCSCONCENCUSRUBNAT ----
    const naturezas = unicos(
      (await baixarTudo(SQL_RUBRICAS_NATUREZAS))
        .filter(o => num(o.NUMCONTRATO) !== null && num(o.CODTCSCONCENCUS) !== null && num(o.CODTCSCONCENCUSRUB) !== null && num(o.CODTCSCONCENCUSRUBNAT) !== null)
        .map(o => ({
          numcontrato: num(o.NUMCONTRATO),
          codtcsconcencus: num(o.CODTCSCONCENCUS),
          codtcsconcencusrub: num(o.CODTCSCONCENCUSRUB),
          codtcsconcencusrubnat: num(o.CODTCSCONCENCUSRUBNAT),
          codnat: num(o.CODNAT),
          extraorc: txt(o.EXTRAORC),
          synced_at: inicioIso,
        })),
      l => `${l.numcontrato}|${l.codtcsconcencus}|${l.codtcsconcencusrub}|${l.codtcsconcencusrubnat}`
    );

    // ---- AD_TCSCONCENCUSRUBPLA ----
    const planejado = unicos(
      (await baixarTudo(SQL_PLANEJADO))
        .filter(o => num(o.NUMCONTRATO) !== null && num(o.CODTCSCONCENCUS) !== null && num(o.CODTCSCONCENCUSRUB) !== null && num(o.CODTCSCONCENCUSRUBPLA) !== null)
        .map(o => ({
          numcontrato: num(o.NUMCONTRATO),
          codtcsconcencus: num(o.CODTCSCONCENCUS),
          codtcsconcencusrub: num(o.CODTCSCONCENCUSRUB),
          codtcsconcencusrubpla: num(o.CODTCSCONCENCUSRUBPLA),
          dtref: parseSankhyaDate(o.DTREF),
          vlrplanejado: Number(o.VLRPLANEJADO || 0),
          dtcadastro: parseSankhyaDate(o.DTCADASTRO),
          synced_at: inicioIso,
        })),
      l => `${l.numcontrato}|${l.codtcsconcencus}|${l.codtcsconcencusrub}|${l.codtcsconcencusrubpla}`
    );

    // ---- AD_RUBRICA ----
    const adRubricas = unicos(
      (await baixarTudo(SQL_AD_RUBRICA)).filter(o => num(o.CODRUBRICA) !== null).map(o => ({
        codrubrica: num(o.CODRUBRICA),
        nome: txt(o.NOME),
        conta: txt(o.CONTA),
        descritivo: txt(o.DESCRITIVO),
        synced_at: inicioIso,
      })),
      l => String(l.codrubrica)
    );

    // Só grava depois de baixar e validar as 6 tabelas: um retorno suspeito em qualquer uma
    // aborta a carga inteira sem tocar em nenhuma.
    const cargas: [string, Record<string, any>[], string, string][] = [
      ['sankhya_contratos', contratos, 'numcontrato', 'numcontrato'],
      ['sankhya_contratos_cencus', cencus, 'numcontrato,codtcsconcencus', 'numcontrato'],
      ['sankhya_contratos_rubricas', rubricas, 'numcontrato,codtcsconcencus,codtcsconcencusrub', 'numcontrato'],
      ['sankhya_contratos_rubricas_naturezas', naturezas, 'numcontrato,codtcsconcencus,codtcsconcencusrub,codtcsconcencusrubnat', 'numcontrato'],
      ['sankhya_contratos_planejado', planejado, 'numcontrato,codtcsconcencus,codtcsconcencusrub,codtcsconcencusrubpla', 'numcontrato'],
      ['sankhya_rubricas', adRubricas, 'codrubrica', 'codrubrica'],
    ];
    for (const [tabela, linhas] of cargas) await validarVolume(tabela, linhas.length);
    for (const [tabela, linhas, onConflict, colunaSelect] of cargas) await gravarTabela(tabela, linhas, onConflict, colunaSelect);

    const receita = contratos.filter(c => c.recdesp === 1 && c.codemp !== 8);
    const segundos = Math.round((Date.now() - inicio) / 1000);
    await writeLog(
      `Ciclo dos contratos atualizado em ${segundos}s: ${contratos.length} contratos (${receita.length} de receita, ${receita.filter(c => c.ativo).length} ativos), ` +
        `${cencus.length} CRs, ${rubricas.length} rubricas, ${naturezas.length} naturezas, ${planejado.length} planejados, ${adRubricas.length} rubricas de cadastro.`,
      'success'
    );

    return new Response(JSON.stringify({
      success: true,
      contratos: contratos.length,
      contratos_receita: receita.length,
      cencus: cencus.length,
      rubricas: rubricas.length,
      naturezas: naturezas.length,
      planejado: planejado.length,
      ad_rubricas: adRubricas.length,
      segundos
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (error: any) {
    const msg = error?.message || String(error);
    console.error("Erro na Edge Function sync-ciclo-contratos:", error);
    try { await writeLog(`Ciclo dos contratos: erro: ${msg}`, 'error'); } catch (_) { /* ignore */ }
    return new Response(JSON.stringify({ success: false, error: msg }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });
  }
});
