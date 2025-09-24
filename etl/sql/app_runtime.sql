-- etl/sql/app_runtime.sql
-- 👇 tabelas apenas do nosso app (não mexem nas tabelas que você já usa)
-- registram o andamento "local" dos setores (Pintura no começo) e um log de eventos

CREATE TABLE IF NOT EXISTS app_setor_exec (
  op_numero     INTEGER NOT NULL,
  setor_codigo  INTEGER NOT NULL,          -- 4 = Pintura
  status_setor  VARCHAR(20) NOT NULL,      -- PENDENTE | EM_EXECUCAO | CONCLUIDO
  dt_inicio     TIMESTAMP NULL,
  dt_fim        TIMESTAMP NULL,
  usuario       TEXT NULL,
  obs           TEXT NULL,
  PRIMARY KEY (op_numero, setor_codigo)
);

CREATE TABLE IF NOT EXISTS app_event (
  id            BIGSERIAL PRIMARY KEY,
  ts            TIMESTAMP NOT NULL DEFAULT now(),
  op_numero     INTEGER NOT NULL,
  setor_codigo  INTEGER NOT NULL,
  event         VARCHAR(40) NOT NULL,      -- INICIAR_PINTURA | FINALIZAR_PINTURA | ...
  usuario       TEXT NULL,
  payload       JSONB NULL
);
