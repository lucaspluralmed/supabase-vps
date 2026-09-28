# supabase-vps

Supabase self-hosted da Pluralmed, implantado no Easypanel como servico Compose (fonte git = este repositorio).

- Base: `docker/` oficial do supabase/supabase (via easypanel-io/compose, ref 18-05-2026), sem `container_name` e sem `ports`.
- Postgres 17.6 (`supabase/postgres:17.6.1.084`), igual a producao hosted.
- Edge Functions do sistema financeiro em `volumes/functions/<nome>/index.ts` (servidas em `/functions/v1/<nome>`).
- Segredos (JWT, senhas, SANKHYA_API_KEY, CRON_SECRET) ficam SOMENTE no env do servico no Easypanel; nada de segredo neste repo.

Deploy: `git push` + Deploy no Easypanel. As functions sao lidas do volume, sem build.

## Backup

Servico `backup` no compose: `pg_dump -Fc` diario (BACKUP_HOUR_UTC, padrao 06:00 UTC = 03:00 Brasilia) de todo o banco,
copia local em `volumes/backup/` (7 dias) e upload para S3 quando `BACKUP_S3_ENDPOINT`, `BACKUP_S3_BUCKET`,
`BACKUP_S3_ACCESS_KEY` e `BACKUP_S3_SECRET_KEY` estiverem definidos no env do Easypanel (retencao BACKUP_RETENTION_DAYS, padrao 30).
Cada execucao fica registrada em `ops.backup_log`. Ao subir, roda um backup imediato (BACKUP_RUN_ON_START).

Restaurar: `pg_restore -h db -U supabase_admin -d postgres --clean --if-exists --no-owner --no-privileges arquivo.dump`.
