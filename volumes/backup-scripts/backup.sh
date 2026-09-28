#!/usr/bin/env bash
# Backup logico diario do Postgres do Supabase self-hosted.
# - pg_dump -Fc (comprimido) de TODO o banco (public, auth, storage, cron...) como supabase_admin
# - copia local em /backup (retencao BACKUP_LOCAL_RETENTION_DAYS)
# - upload para S3 (path-style, SigV4 via curl) se BACKUP_S3_* estiverem definidos, com retencao BACKUP_RETENTION_DAYS
# - registra cada execucao em ops.backup_log (consultavel pelo Studio / /pg/query)
set -u
export PGHOST="${PGHOST:-db}" PGUSER="${PGUSER:-supabase_admin}" PGDATABASE="${PGDATABASE:-postgres}"
RUN_ON_START="${BACKUP_RUN_ON_START:-true}"
HOUR_UTC="${BACKUP_HOUR_UTC:-06}"
LOCAL_DIR="/backup"
LOCAL_RET="${BACKUP_LOCAL_RETENTION_DAYS:-7}"
S3_RET="${BACKUP_RETENTION_DAYS:-30}"
S3_ENDPOINT="${BACKUP_S3_ENDPOINT:-}"      # ex.: https://s3.us-east-1.amazonaws.com  ou  https://s3.us-west-004.backblazeb2.com
S3_REGION="${BACKUP_S3_REGION:-us-east-1}"
S3_BUCKET="${BACKUP_S3_BUCKET:-}"
S3_KEY="${BACKUP_S3_ACCESS_KEY:-}"
S3_SECRET="${BACKUP_S3_SECRET_KEY:-}"
S3_PREFIX="${BACKUP_S3_PREFIX:-supabase-financeiro}"

log() { echo "[$(date -u +%FT%TZ)] $*"; }

ensure_tools() {
  if ! command -v curl >/dev/null 2>&1; then
    log "instalando curl"; apt-get update -qq >/dev/null && apt-get install -y -qq curl >/dev/null
  fi
}

ensure_table() {
  psql -v ON_ERROR_STOP=0 -q -c "CREATE SCHEMA IF NOT EXISTS ops;
    CREATE TABLE IF NOT EXISTS ops.backup_log (
      id bigserial primary key, executado_em timestamptz default now(), arquivo text, bytes bigint,
      destino text, status text, detalhe text);
    REVOKE ALL ON SCHEMA ops FROM anon, authenticated;" >/dev/null 2>&1 || true
}

registrar() { # arquivo bytes destino status detalhe
  psql -q -c "INSERT INTO ops.backup_log(arquivo, bytes, destino, status, detalhe) VALUES ('$1', $2, '$3', '$4', \$d\$$5\$d\$)" >/dev/null 2>&1 || true
}

s3_curl() { # metodo url [extra args...]
  local m="$1" u="$2"; shift 2
  curl -sS --fail-with-body --aws-sigv4 "aws:amz:${S3_REGION}:s3" --user "${S3_KEY}:${S3_SECRET}" -X "$m" "$u" "$@"
}

s3_upload() { # arquivo_local nome
  s3_curl PUT "${S3_ENDPOINT}/${S3_BUCKET}/${S3_PREFIX}/$2" -T "$1" -H "Content-Type: application/octet-stream" -o /dev/null
}

s3_retencao() {
  local cutoff; cutoff=$(date -u -d "-${S3_RET} days" +%Y%m%d)
  local xml; xml=$(s3_curl GET "${S3_ENDPOINT}/${S3_BUCKET}/?list-type=2&prefix=${S3_PREFIX}/" 2>/dev/null) || { log "retencao S3: falha ao listar"; return; }
  echo "$xml" | grep -oE "<Key>[^<]+</Key>" | sed -E 's#</?Key>##g' | while read -r key; do
    d=$(echo "$key" | grep -oE '[0-9]{8}' | head -1)
    if [ -n "$d" ] && [ "$d" -lt "$cutoff" ]; then
      s3_curl DELETE "${S3_ENDPOINT}/${S3_BUCKET}/${key}" -o /dev/null && log "S3: removido $key (retencao ${S3_RET}d)"
    fi
  done
}

fazer_backup() {
  ensure_tools; ensure_table
  local stamp file path bytes
  stamp=$(date -u +%Y%m%d-%H%M); file="financeiro-${stamp}.dump"; path="${LOCAL_DIR}/${file}"
  log "iniciando pg_dump -> $file"
  if ! pg_dump -Fc --no-owner --no-privileges -f "$path"; then
    log "ERRO no pg_dump"; registrar "$file" 0 "local" "erro" "pg_dump falhou"; rm -f "$path"; return 1
  fi
  bytes=$(stat -c %s "$path"); log "dump ok: $bytes bytes"
  registrar "$file" "$bytes" "local" "ok" "$LOCAL_DIR"
  find "$LOCAL_DIR" -name 'financeiro-*.dump' -mtime +"$LOCAL_RET" -delete 2>/dev/null
  if [ -n "$S3_ENDPOINT" ] && [ -n "$S3_BUCKET" ] && [ -n "$S3_KEY" ] && [ -n "$S3_SECRET" ]; then
    local out
    if out=$(s3_upload "$path" "$file" 2>&1); then
      log "S3 ok: ${S3_BUCKET}/${S3_PREFIX}/${file}"; registrar "$file" "$bytes" "s3://${S3_BUCKET}/${S3_PREFIX}" "ok" "${S3_ENDPOINT}"
      s3_retencao
    else
      log "S3 ERRO: $out"; registrar "$file" "$bytes" "s3://${S3_BUCKET}/${S3_PREFIX}" "erro" "$out"
    fi
  else
    log "S3 nao configurado (BACKUP_S3_*), backup apenas local"; registrar "$file" "$bytes" "s3" "pulado" "BACKUP_S3_* nao definidos"
  fi
}

segundos_ate_proxima() {
  local now next; now=$(date -u +%s)
  next=$(date -u -d "$(date -u +%F) ${HOUR_UTC}:00:00" +%s)
  [ "$next" -le "$now" ] && next=$((next + 86400))
  echo $((next - now))
}

log "backup service iniciado; hora diaria ${HOUR_UTC}:00 UTC; retencao local ${LOCAL_RET}d, S3 ${S3_RET}d"
until pg_isready -q; do log "aguardando banco"; sleep 10; done
[ "$RUN_ON_START" = "true" ] && fazer_backup
while true; do
  s=$(segundos_ate_proxima); log "proximo backup em ${s}s"; sleep "$s"; fazer_backup
done
