# supabase-vps

Supabase self-hosted da Pluralmed, implantado no Easypanel como servico Compose (fonte git = este repositorio).

- Base: `docker/` oficial do supabase/supabase (via easypanel-io/compose, ref 18-05-2026), sem `container_name` e sem `ports`.
- Postgres 17.6 (`supabase/postgres:17.6.1.084`), igual a producao hosted.
- Edge Functions do sistema financeiro em `volumes/functions/<nome>/index.ts` (servidas em `/functions/v1/<nome>`).
- Segredos (JWT, senhas, SANKHYA_API_KEY, CRON_SECRET) ficam SOMENTE no env do servico no Easypanel; nada de segredo neste repo.

Deploy: `git push` + Deploy no Easypanel. As functions sao lidas do volume, sem build.
