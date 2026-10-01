import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

// ==============================================================================
// SYNC FINANCEIRO UNIFICADO POR PERÍODO
//
// Recarrega a janela escolhida pelo usuário da VW_FINANCEIRO_UNIFICADO para a
// tabela sankhya_financeiro_unificado. Dois modos, pelo corpo do POST:
//   { dias: 30|60|90|120 }  -> competência a partir de hoje - N dias
//   { modo: "incremental" } -> tudo que mudou (DTALTER) ou foi baixado (DATA_BAIXA) desde
//                              ontem, qualquer competência; upsert direto pela hash_id, e
//                              remove os títulos excluídos no Sankhya (TGFFIN_EXC) no período.
//                              Não zera os logs. É o que o pg_cron chama a cada 15 min
//                              (job financeiro-unificado-incremental-15min).
//   { ano, mes }            -> um mês de competência inteiro (do dia 1 ao último).
//                              Não mexe em saldos bancários TGFSBC, que têm
//                              hash_id sem data - ver a migration do intervalo.
//
// Segurança dos dados:
//   1. Baixa TODAS as páginas do Sankhya (com retentativas e backoff) para memória.
//   2. Grava tudo numa tabela staging. A tabela principal continua intocada.
//   3. Só então chama aplicar_sync_unificado_periodo(), que apaga a janela e insere a
//      staging na MESMA transação. Qualquer erro antes disso = nada muda na principal.
// ==============================================================================

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

const DIAS_PERMITIDOS = [30, 60, 90, 120];
// Modo mês: a base unificada começa em julho/2025, então não faz sentido oferecer
// competências anteriores. O teto acompanha o ano corrente + 2 para alcançar as
// provisões lançadas com competência futura.
const COMPETENCIA_MIN = { ano: 2025, mes: 7 };
const ANOS_FUTUROS_PERMITIDOS = 2;
const PAGE_SIZE = 1500;
const STAGING_CHUNK = 1000;
const MAX_TENTATIVAS = 6;
// Edge Functions são derrubadas em 150s. Acima disso abortamos ANTES de tocar na principal.
const ORCAMENTO_MS = 125_000;
// Se o Sankhya devolver menos da metade do que temos localmente na janela, algo está
// errado na API (retorno parcial). Abortamos em vez de apagar dados bons.
const PROPORCAO_MINIMA_REMOTA = 0.5;

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

// Data de hoje no fuso da empresa (Fortaleza) menos N dias, em YYYY-MM-DD.
function calcularDataCorte(dias: number): string {
  const hoje = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Fortaleza', year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(new Date()); // en-CA => YYYY-MM-DD
  const [y, m, d] = hoje.split('-').map(Number);
  const corte = new Date(Date.UTC(y, m - 1, d));
  corte.setUTCDate(corte.getUTCDate() - dias);
  return corte.toISOString().substring(0, 10);
}

// Ano corrente no fuso da empresa, usado como base do teto de competência.
function anoAtual(): number {
  return Number(new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Fortaleza', year: 'numeric'
  }).format(new Date()));
}

// Valida o mês de competência pedido. Devolve a mensagem de erro ou null.
function validarCompetencia(ano: number, mes: number): string | null {
  if (!Number.isInteger(ano) || !Number.isInteger(mes)) {
    return "Informe 'ano' e 'mes' como números inteiros.";
  }
  if (mes < 1 || mes > 12) return `Mês inválido: ${mes}.`;
  if (ano < COMPETENCIA_MIN.ano || (ano === COMPETENCIA_MIN.ano && mes < COMPETENCIA_MIN.mes)) {
    return `A base começa em ${String(COMPETENCIA_MIN.mes).padStart(2, '0')}/${COMPETENCIA_MIN.ano}. Competências anteriores não podem ser atualizadas por aqui.`;
  }
  const anoMaximo = anoAtual() + ANOS_FUTUROS_PERMITIDOS;
  if (ano > anoMaximo) return `Competência muito no futuro. O limite é dezembro de ${anoMaximo}.`;
  return null;
}

// Primeiro e último dia do mês, em YYYY-MM-DD. Date.UTC(ano, mes, 0) cai no último
// dia do mês informado porque 'mes' aqui é 1-based.
function limitesDoMes(ano: number, mes: number): { inicio: string; fim: string } {
  const mm = String(mes).padStart(2, '0');
  const ultimoDia = new Date(Date.UTC(ano, mes, 0)).getUTCDate();
  return { inicio: `${ano}-${mm}-01`, fim: `${ano}-${mm}-${String(ultimoDia).padStart(2, '0')}` };
}

function ehErroDeLimite(msg: string): boolean {
  const m = msg.toLowerCase();
  return m.includes('429') || m.includes('too many') || m.includes('rate') ||
    m.includes('limit') || m.includes('limite') || m.includes('excedid') ||
    m.includes('503') || m.includes('502');
}

const sleep = (ms: number) => new Promise(res => setTimeout(res, ms));

serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  const inicio = Date.now();
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
  const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
  const supabase = createClient(supabaseUrl, supabaseKey);

  let tipoLogAtual = 'unificado_periodo';
  const writeLog = async (message: string, level: string = 'info', syncType: string = tipoLogAtual) => {
    console.log(`[${level.toUpperCase()}] ${message}`);
    await supabase.from('sync_logs').insert({ message, level, sync_type: syncType });
  };

  try {
    // ---------------- Autorização ----------------
    const authHeader = req.headers.get('Authorization');
    // Sem fallback literal: um segredo versionado no git nao e segredo.
    // Se CRON_SECRET nao estiver configurado, o caminho do cron fica
    // desabilitado em vez de virar "Bearer " (que qualquer um enviaria).
    // Dois segredos válidos para o cron: a variável de ambiente CRON_SECRET (legado) e o
    // segredo do cofre do Supabase (vault), que é o que o pg_cron lê ao montar o header.
    // Ver migration ..._cron_incremental_unificado.
    const segredosCron: string[] = [];
    const cronSecretEnv = Deno.env.get("CRON_SECRET") ?? "";
    if (cronSecretEnv) segredosCron.push(cronSecretEnv);
    const { data: segredoVault } = await supabase.rpc('segredo_cron_sync');
    if (typeof segredoVault === 'string' && segredoVault) segredosCron.push(segredoVault);

    let isAuthorized = false;
    if (authHeader && segredosCron.some(s => authHeader === `Bearer ${s}`)) {
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

    // ---------------- Parâmetros ----------------
    let body: any = {};
    try { body = await req.json(); } catch (_) { body = {}; }

    const responderErro = (error: string) => new Response(JSON.stringify({ error }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" }
    });

    // Dois modos:
    //   a) janela fixa terminando hoje -> { dias: 30|60|90|120 }
    //   b) mês de competência inteiro  -> { ano, mes }
    const modoIncremental = body?.modo === 'incremental' || body?.incremental === true;
    const modoIntervalo = !modoIncremental && (body?.ano != null || body?.mes != null);

    let dias: number | null = null;
    let ano: number | null = null;
    let mes: number | null = null;
    let dataCorte = '';
    let dataFim: string | null = null;

    if (modoIncremental) {
      tipoLogAtual = 'unificado_incremental';
    } else if (modoIntervalo) {
      ano = Number(body?.ano);
      mes = Number(body?.mes);
      const erro = validarCompetencia(ano, mes);
      if (erro) return responderErro(erro);
      const limites = limitesDoMes(ano, mes);
      dataCorte = limites.inicio;
      dataFim = limites.fim;
    } else {
      dias = Number(body?.dias);
      if (!DIAS_PERMITIDOS.includes(dias)) {
        return responderErro(
          `Parâmetro 'dias' inválido. Use um destes valores: ${DIAS_PERMITIDOS.join(', ')}, ou envie 'ano' e 'mes'.`
        );
      }
      dataCorte = calcularDataCorte(dias);
    }

    const sankhyaToken = Deno.env.get("SANKHYA_API_KEY");
    if (!sankhyaToken) throw new Error("SANKHYA_API_KEY não encontrada.");

    // Saldos bancários (TGFSBC) têm hash_id sem data, então a última ocorrência da
    // janela é o que fica. Isso só é correto quando a janela termina hoje; num
    // intervalo passado gravaria um saldo velho. Ver a migration ..._por_intervalo.
    const competenciaBR = modoIntervalo ? `${String(mes).padStart(2, '0')}/${ano}` : '';
    const descricaoJanela = modoIntervalo
      ? `competência de ${competenciaBR} (${dataCorte} a ${dataFim})`
      : `competência >= ${dataCorte}`;

    let localCount: number | null = null;
    if (!modoIncremental) {
      // Toda execução por janela começa com a tabela de logs zerada (o modal da tela mostra só a carga atual)
      const { error: clearLogsError } = await supabase
        .from('sync_logs').delete().gte('created_at', '1970-01-01');
      if (clearLogsError) console.warn(`[WARN] Não foi possível limpar sync_logs: ${clearLogsError.message}`);

      await writeLog(
        modoIntervalo
          ? `Atualização da competência ${competenciaBR} solicitada (${dataCorte} a ${dataFim}). Saldos bancários (TGFSBC) não são alterados neste modo.`
          : `Atualização dos últimos ${dias} dias solicitada. Data de corte: ${dataCorte} (${descricaoJanela}).`
      );

      // ---------------- Quanto temos localmente na janela ----------------
      let consultaLocal = supabase
        .from('sankhya_financeiro_unificado')
        .select('hash_id', { count: 'exact', head: true })
        .gte('data_competencia', dataCorte);
      if (modoIntervalo) {
        consultaLocal = consultaLocal
          .lte('data_competencia', dataFim!)
          .neq('origem_registro', 'TGFSBC');
      }
      const { count, error: localErr } = await consultaLocal;
      if (localErr) throw new Error(`Erro ao contar registros locais: ${localErr.message}`);
      localCount = count;
      await writeLog(`${localCount ?? 0} registros locais na janela serão substituídos SOMENTE se a carga do Sankhya concluir com sucesso.`);
    }

    // ---------------- Consulta ao Sankhya com retentativas ----------------
    async function executeQuery(query: string): Promise<any> {
      let ultimoErro = '';
      for (let tentativa = 1; tentativa <= MAX_TENTATIVAS; tentativa++) {
        if (Date.now() - inicio > ORCAMENTO_MS) {
          throw new Error(`Tempo limite da função atingido antes de concluir a carga (${Math.round((Date.now() - inicio) / 1000)}s). Nenhum dado foi alterado.`);
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
          // Backoff exponencial: 2s, 4s, 8s, 16s, 30s. Limite de requisições espera mais.
          let espera = Math.min(2000 * Math.pow(2, tentativa - 1), 30_000);
          if (ehErroDeLimite(ultimoErro)) espera = Math.min(espera * 1.5, 30_000);
          await writeLog(`Tentativa ${tentativa}/${MAX_TENTATIVAS} falhou (${ultimoErro}). Aguardando ${Math.round(espera / 1000)}s...`, 'warning');
          await sleep(espera);
        }
      }
      throw new Error(`API do Sankhya falhou após ${MAX_TENTATIVAS} tentativas: ${ultimoErro}`);
    }

    // ---------------- Formatação ----------------
    const formatRow = (rowArray: any[], fields: string[]) => {
      const obj: Record<string, any> = {};
      fields.forEach((field: string, i: number) => { obj[field] = rowArray[i]; });

      const origem = obj.ORIGEM_REGISTRO || 'TGFFIN';
      const idSankhya = obj.ID !== null && obj.ID !== undefined ? obj.ID : 0;
      let hashId = `${origem}_${idSankhya}`;
      if (origem === 'TGFSBC') hashId = `TGFSBC_${obj.COD_CONTA_BANCARIA || 0}`;

      const num = (v: any) => (v !== null && v !== undefined ? Number(v) : 0);

      return {
        hash_id: hashId,
        origem_registro: origem,
        id_sankhya: Number(idSankhya),
        codemp: Number(obj.CODEMP || 0),
        empresa: obj.EMPRESA,
        codctabcoint: obj.COD_CONTA_BANCARIA ? Number(obj.COD_CONTA_BANCARIA) : null,
        codctabco: obj.CODCTABCO ? String(obj.CODCTABCO) : null,
        conta_bancaria: obj.CONTA_BANCARIA,
        classe_conta: obj.CLASSE_CONTA,
        codcencus: obj.CODCENCUS ? Number(obj.CODCENCUS) : null,
        centro_custos: obj.CENTRO_CUSTOS,
        codnat: obj.CODNAT ? Number(obj.CODNAT) : null,
        natureza: obj.NATUREZA,
        grupo_dfc: obj.GRUPO_DFC,
        nivel_1: obj.NIVEL_1,
        nivel_2: obj.NIVEL_2,
        nivel_3: obj.NIVEL_3,
        nivel_4: obj.NIVEL_4,
        codparc: obj.CODPARC ? Number(obj.CODPARC) : null,
        parceiro: obj.PARCEIRO,
        nf: obj.NF ? String(obj.NF) : null,
        cod_hist: obj.COD_HIST,
        historico: obj.HISTORICO,
        data_competencia: parseSankhyaDate(obj.DATA_COMPETENCIA, false),
        data_baixa: parseSankhyaDate(obj.DATA_BAIXA, false),
        data_vencimento: parseSankhyaDate(obj.DATA_VENCIMENTO, false),
        valor_original: num(obj.VALOR_ORIGINAL),
        valor_baixa: num(obj.VALOR_BAIXA),
        status: obj.STATUS,
        provisionado_real: obj.PROVISIONADO_REAL,
        tipo_lancamento: obj.TIPO_LANCAMENTO,
        valor_imposto: num(obj.VALOR_IMPOSTO),
        valor_liquido: num(obj.VALOR_LIQUIDO),
        emenda: obj.EMENDA?.toUpperCase() === 'SIM',
        dtalter: parseSankhyaDate(obj.DTALTER, true),
        usuario: obj.USUARIO,
        usuario_baixa: obj.USUARIO_BAIXA,
        conta: obj.CONTA ? String(obj.CONTA) : null,
        nome_rubrica: obj.NOME_RUBRICA ? String(obj.NOME_RUBRICA) : null,
        chave_pix: obj.CHAVE_PIX ? String(obj.CHAVE_PIX) : null,
        multa_juros: num(obj.MULTA_JUROS),
      };
    };

    // ---------------- MODO INCREMENTAL (pg_cron a cada 15 min) ----------------
    // Tudo que mudou no Sankhya (DTALTER) ou foi baixado (DATA_BAIXA) desde ontem 00:00,
    // qualquer competência, exceto saldos bancários (TGFSBC não tem DTALTER e é tratado pela
    // carga por janela / sync-saldo-bancario). Upsert direto na principal pela hash_id
    // (origem_id, estável): idempotente. Depois remove da base os títulos apagados no Sankhya
    // desde o corte (TGFFIN_EXC). Como todo fluxo, começa zerando sync_logs.
    if (modoIncremental) {
      const corte = calcularDataCorte(1);
      const { error: limpaLogs } = await supabase.from('sync_logs').delete().gte('created_at', '1970-01-01');
      if (limpaLogs) console.warn(`[WARN] Não foi possível limpar sync_logs: ${limpaLogs.message}`);
      await writeLog(`Incremental: buscando alterações e baixas desde ${corte}...`);

      const filtroIncremental = `(DTALTER >= TO_DATE('${corte}', 'YYYY-MM-DD') OR DATA_BAIXA >= TO_DATE('${corte}', 'YYYY-MM-DD'))
         AND (ORIGEM_REGISTRO IS NULL OR ORIGEM_REGISTRO <> 'TGFSBC')`;
      const porHashIncremental = new Map<string, Record<string, any>>();
      let recebidas = 0;
      let deslocamento = 0;
      let paginas = 0;
      let continua = true;
      while (continua) {
        paginas++;
        const sql = `
          SELECT * FROM VW_FINANCEIRO_UNIFICADO
          WHERE ${filtroIncremental}
          ORDER BY DTALTER ASC, ID ASC
          OFFSET ${deslocamento} ROWS FETCH NEXT ${PAGE_SIZE} ROWS ONLY
        `;
        const rb = await executeQuery(sql);
        const rows: any[] = rb?.rows || [];
        if (rows.length === 0) break;
        const fields: string[] = rb.fieldsMetadata.map((f: any) => f.name);
        for (const r of rows) {
          const row = formatRow(r, fields);
          porHashIncremental.set(row.hash_id, row);
        }
        recebidas += rows.length;
        deslocamento += rows.length;
        if (rows.length < PAGE_SIZE) continua = false;
      }

      const registrosIncremental = Array.from(porHashIncremental.values());
      let gravados = 0;
      for (let i = 0; i < registrosIncremental.length; i += 500) {
        if (Date.now() - inicio > ORCAMENTO_MS) {
          throw new Error(`Tempo limite atingido no incremental após ${gravados} de ${registrosIncremental.length} registros; o restante entra na próxima rodada.`);
        }
        const chunk = registrosIncremental.slice(i, i + 500);
        const { error: upErr } = await supabase
          .from('sankhya_financeiro_unificado')
          .upsert(chunk, { onConflict: 'hash_id' });
        if (upErr) throw new Error(`Erro no upsert incremental: ${upErr.message}`);
        gravados += chunk.length;
      }

      // Excluídos: títulos apagados no Sankhya desde o corte (TGFFIN_EXC) saem da base.
      // Guarda: só NUFIN que realmente não existe mais em TGFFIN, para nunca apagar título vivo.
      // hash_id de TGFFIN é `TGFFIN_<NUFIN>` (ver formatRow).
      let excluidos = 0;
      const nufinsExcluidos: number[] = [];
      let deslocamentoExc = 0;
      for (;;) {
        const sqlExc = `
          SELECT E.NUFIN FROM TGFFIN_EXC E
          WHERE E.DHEXCLUSAO >= TO_DATE('${corte}', 'YYYY-MM-DD')
            AND NOT EXISTS (SELECT 1 FROM TGFFIN F WHERE F.NUFIN = E.NUFIN)
          ORDER BY E.NUFIN
          OFFSET ${deslocamentoExc} ROWS FETCH NEXT ${PAGE_SIZE} ROWS ONLY
        `;
        const rbExc = await executeQuery(sqlExc);
        const rowsExc: any[] = rbExc?.rows || [];
        if (rowsExc.length === 0) break;
        rowsExc.forEach(r => { const n = Number(r[0]); if (Number.isInteger(n) && n > 0) nufinsExcluidos.push(n); });
        deslocamentoExc += rowsExc.length;
        if (rowsExc.length < PAGE_SIZE) break;
      }
      for (let i = 0; i < nufinsExcluidos.length; i += 500) {
        const hashes = nufinsExcluidos.slice(i, i + 500).map(n => `TGFFIN_${n}`);
        const { data: apagados, error: delErr } = await supabase
          .from('sankhya_financeiro_unificado')
          .delete()
          .in('hash_id', hashes)
          .select('hash_id');
        if (delErr) throw new Error(`Erro ao remover excluídos: ${delErr.message}`);
        excluidos += apagados?.length ?? 0;
      }

      const segundosIncremental = Math.round((Date.now() - inicio) / 1000);
      await writeLog(
        `Incremental concluído em ${segundosIncremental}s: ${recebidas} linhas em ${paginas} página(s), ${gravados} registros atualizados, ${excluidos} removidos (${nufinsExcluidos.length} excluídos no Sankhya desde ${corte}).`,
        'success'
      );
      return new Response(JSON.stringify({
        success: true,
        modo: 'incremental',
        data_corte: corte,
        remotos: registrosIncremental.length,
        inseridos: gravados,
        excluidos_sankhya: nufinsExcluidos.length,
        removidos: excluidos,
        segundos: segundosIncremental
      }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ---------------- FASE 1: baixar toda a janela para memória ----------------
    await writeLog(`Fase 1/3: baixando do Sankhya os lançamentos com ${descricaoJanela}...`);

    // As duas datas passaram pelo regex YYYY-MM-DD (ou saíram de calcularDataCorte),
    // então entram no SQL sem risco de injeção.
    const filtroSql = modoIntervalo
      ? `DATA_COMPETENCIA BETWEEN TO_DATE('${dataCorte}', 'YYYY-MM-DD') AND TO_DATE('${dataFim}', 'YYYY-MM-DD')
         AND (ORIGEM_REGISTRO IS NULL OR ORIGEM_REGISTRO <> 'TGFSBC')`
      : `DATA_COMPETENCIA >= TO_DATE('${dataCorte}', 'YYYY-MM-DD')`;

    // Map por hash_id: a mesma chave pode vir mais de uma vez (ex.: saldo bancário
    // TGFSBC por conta em vários meses). Fica a última ocorrência, que é a mais recente
    // porque a consulta é ordenada por DATA_COMPETENCIA ASC.
    const porHash = new Map<string, Record<string, any>>();
    let linhasRecebidas = 0;
    let offset = 0;
    let pagina = 0;
    let hasMore = true;

    while (hasMore) {
      pagina++;
      const sql = `
        SELECT * FROM VW_FINANCEIRO_UNIFICADO
        WHERE ${filtroSql}
        ORDER BY DATA_COMPETENCIA ASC, ID ASC
        OFFSET ${offset} ROWS FETCH NEXT ${PAGE_SIZE} ROWS ONLY
      `;
      const responseBody = await executeQuery(sql);
      const rows: any[] = responseBody?.rows || [];

      if (rows.length === 0) {
        hasMore = false;
        break;
      }

      const fields: string[] = responseBody.fieldsMetadata.map((f: any) => f.name);
      for (const r of rows) {
        const row = formatRow(r, fields);
        porHash.set(row.hash_id, row);
      }
      linhasRecebidas += rows.length;
      offset += rows.length;
      await writeLog(`Página ${pagina}: ${rows.length} linhas recebidas. Total: ${linhasRecebidas}.`);

      if (rows.length < PAGE_SIZE) hasMore = false;
    }

    const registros = Array.from(porHash.values());

    // ---------------- Guardas: não apagar dados bons por retorno ruim da API ----------------
    if (registros.length === 0) {
      // Intervalo escolhido a dedo pode ser legitimamente vazio dos dois lados
      // (mês sem lançamento). Só é erro quando existe dado local que sumiria.
      if ((localCount ?? 0) === 0) {
        await writeLog(`Nenhum lançamento no Sankhya e nenhum local para ${descricaoJanela}. Nada a fazer.`, 'success');
        return new Response(JSON.stringify({
          success: true,
          modo: modoIntervalo ? 'competencia' : 'a_partir_de',
          dias, ano, mes, data_corte: dataCorte, data_fim: dataFim,
          local_antes: 0, remotos: 0, deletados: 0, inseridos: 0,
          segundos: Math.round((Date.now() - inicio) / 1000)
        }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }
      throw new Error(`O Sankhya não retornou nenhum lançamento para a janela (${descricaoJanela}), mas existem ${localCount} registros locais. Nada foi alterado.`);
    }
    const minimoEsperado = Math.floor((localCount ?? 0) * PROPORCAO_MINIMA_REMOTA);
    if (registros.length < minimoEsperado) {
      throw new Error(`Retorno suspeito da API: ${registros.length} registros recebidos contra ${localCount} existentes na janela (mínimo aceito: ${minimoEsperado}). Nada foi alterado.`);
    }

    await writeLog(`Fase 1 concluída: ${linhasRecebidas} linhas em ${pagina} página(s), ${registros.length} registros únicos.`, 'success');

    // ---------------- FASE 2: gravar na staging ----------------
    await writeLog(`Fase 2/3: gravando ${registros.length} registros na área de espera (staging)...`);

    const { error: limpaStgErr } = await supabase
      .from('sankhya_financeiro_unificado_staging').delete().gte('codemp', -1);
    if (limpaStgErr) throw new Error(`Erro ao limpar staging: ${limpaStgErr.message}`);

    let gravados = 0;
    for (let i = 0; i < registros.length; i += STAGING_CHUNK) {
      if (Date.now() - inicio > ORCAMENTO_MS) {
        throw new Error(`Tempo limite da função atingido durante a gravação na staging. Nenhum dado foi alterado na tabela principal.`);
      }
      const chunk = registros.slice(i, i + STAGING_CHUNK);
      const { error: insErr } = await supabase
        .from('sankhya_financeiro_unificado_staging').insert(chunk);
      if (insErr) throw new Error(`Erro ao gravar na staging: ${insErr.message}`);
      gravados += chunk.length;
    }
    await writeLog(`Fase 2 concluída: ${gravados} registros na staging.`, 'success');

    // ---------------- FASE 3: troca atômica ----------------
    await writeLog(`Fase 3/3: aplicando na tabela principal (apagar janela + inserir) em uma única transação...`);

    const { data: resultado, error: rpcErr } = await supabase
      .rpc('aplicar_sync_unificado_periodo', { p_data_corte: dataCorte, p_data_fim: dataFim });
    if (rpcErr) throw new Error(`Erro ao aplicar a troca atômica (transação desfeita, nada alterado): ${rpcErr.message}`);

    const segundos = Math.round((Date.now() - inicio) / 1000);
    const janela = modoIntervalo ? `Competência ${competenciaBR}` : `Janela ${dias} dias (>= ${dataCorte})`;
    await writeLog(
      `Atualização concluída em ${segundos}s! ${janela}: ${resultado?.deletados ?? 0} apagados, ${resultado?.inseridos ?? 0} inseridos/atualizados.`,
      'success'
    );

    return new Response(JSON.stringify({
      success: true,
      modo: modoIntervalo ? 'competencia' : 'a_partir_de',
      dias,
      ano,
      mes,
      data_corte: dataCorte,
      data_fim: dataFim,
      local_antes: resultado?.local_antes ?? localCount ?? 0,
      remotos: registros.length,
      deletados: resultado?.deletados ?? 0,
      inseridos: resultado?.inseridos ?? 0,
      segundos
    }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch (error: any) {
    const msg = error?.message || String(error);
    console.error("Erro na Edge Function sync-financeiro-unificado:", error);
    try { await writeLog(`Erro: ${msg}`, 'error'); } catch (_) { /* ignore */ }
    return new Response(JSON.stringify({ success: false, error: msg }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" }
    });
  }
});
