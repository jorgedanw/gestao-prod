-- schema minimalista para iniciar
CREATE TABLE IF NOT EXISTS op (
  op_id               INTEGER PRIMARY KEY,            -- ORP_ID
  op_numero           INTEGER UNIQUE,                 -- ORP_SERIE (único)
  filial              INTEGER,
  descricao           TEXT,
  pedido_numero       INTEGER,
  status_code         VARCHAR(4),
  status_nome         VARCHAR(40),
  dt_emissao          TIMESTAMP,
  dt_prev_inicio      TIMESTAMP,
  dt_validade         TIMESTAMP,
  qtd_total_hdr       NUMERIC(18,3),
  qtd_produzidas_hdr  NUMERIC(18,3),
  qtd_saldo_hdr       NUMERIC(18,3),
  percent_concluido   NUMERIC(7,2),
  cor_txt             VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS op_item (
  opd_id          INTEGER PRIMARY KEY,            -- OPD_ID
  op_id           INTEGER REFERENCES op(op_id) ON DELETE CASCADE,
  op_numero       INTEGER,
  lote            INTEGER,
  pro_codigo      INTEGER,
  cor_codigo      INTEGER,
  qtd             NUMERIC(18,3),
  qtd_produzidas  NUMERIC(18,3),
  qtd_saldo       NUMERIC(18,3),
  pro_desc        TEXT,                           -- NOVO
  cor_nome        VARCHAR(200)                    -- NOVO (nome exato da CORES)
);

CREATE TABLE IF NOT EXISTS roteiro (
  id              BIGSERIAL PRIMARY KEY,
  op_numero       INTEGER NOT NULL,
  setor_codigo    INTEGER NOT NULL,
  sequencia       INTEGER NOT NULL,
  UNIQUE (op_numero, setor_codigo, sequencia)
);

-- status por setor/etapa (derivado do PCP_*ROTEIRO do Microsys)
CREATE TABLE IF NOT EXISTS andamento_setor (
  op_numero    INTEGER NOT NULL,
  setor_codigo INTEGER NOT NULL,
  sequencia    INTEGER NOT NULL,
  status_setor VARCHAR(20) NOT NULL,   -- PENDENTE | EM_EXECUCAO | CONCLUIDO
  dt_inicio    TIMESTAMP NULL,
  dt_fim       TIMESTAMP NULL,
  PRIMARY KEY (op_numero, setor_codigo, sequencia)
);

-- Índices úteis
CREATE INDEX IF NOT EXISTS idx_andamento_op            ON andamento_setor(op_numero);
CREATE INDEX IF NOT EXISTS idx_op_op_numero            ON op(op_numero);
CREATE INDEX IF NOT EXISTS idx_item_op                 ON op_item(op_id);
CREATE INDEX IF NOT EXISTS idx_rot_op                  ON roteiro(op_numero);

-- Janelas por data
CREATE INDEX IF NOT EXISTS idx_op_filial_validade      ON op (filial, dt_validade);
CREATE INDEX IF NOT EXISTS idx_op_filial_prev_inicio   ON op (filial, dt_prev_inicio);
CREATE INDEX IF NOT EXISTS idx_op_filial_emissao       ON op (filial, dt_emissao);

-- Itens (consultas/fallback de pintura)
CREATE INDEX IF NOT EXISTS idx_item_opnum_procod       ON op_item (op_numero, pro_codigo);
CREATE INDEX IF NOT EXISTS idx_item_opid_saldo         ON op_item (op_id) WHERE qtd_saldo > 0;

-- Migração segura (ambientes já criados)
ALTER TABLE op_item ADD COLUMN IF NOT EXISTS pro_desc  TEXT;
ALTER TABLE op_item ADD COLUMN IF NOT EXISTS cor_nome  VARCHAR(200);

-- Produtos que representam "pintura"
CREATE TABLE IF NOT EXISTS cfg_pintura_prod (
  pro_codigo INTEGER PRIMARY KEY,
  observacao TEXT
);
-- Cadastro de itens que representam etapa de PINTURA
-- Produtos que representam "pintura"
CREATE TABLE IF NOT EXISTS cfg_pintura_prod (
  pro_codigo INTEGER PRIMARY KEY,
  observacao TEXT
);

INSERT INTO cfg_pintura_prod (pro_codigo, observacao) VALUES
  (373, 'FUNDO ESPECIAL 6,5'),
  (210, 'CERÂMICA'),
  (149, 'BRONZE 1003'),
  (134, 'PRETO FOSCO'),
  (143, 'AMARELO SEGURANÇA'),
  (662, 'AZUL PBJ125'),
  (140, 'AZUL DEL REY PBJ072'),
  (934, 'AZUL DEL REY BRASICOAT'),
  (827, 'AZUL FRANÇA RAL 5015'),
  (141, 'AZUL REAL PBJ035'),
  (160, 'BRANCO RAL 9003'),
  (136, 'CINZA 6,5'),
  (617, 'CINZA 7021'),
  (138, 'CINZA 7037'),
  (139, 'CINZA 7037 FOSCO'),
  (892, 'CINZA 7043'),
  (148, 'MARROM 8014'),
  (150, 'PRATA BANCO'),   -- confirme se é “BRANCO” mesmo
  (389, 'VERDE PBK 038 (VERDE ÁGUA)'),
  (145, 'VERDE PBK 041 (FOLHA)'),
  (863, 'VERDE SÃO MIGUEL'),
  (151, 'VERDE SICREDI'),
  (147, 'VERMELHO'),
  (223, 'CINZA GRAFITE 7016'),
  (137, 'CINZA GRAFITE 7024'),
  (142, 'LARANJA'),
  (135, 'PRETO BRILHO FBN001'),
  (144, 'BEGE MÉDIO FBD001'),
  (869, 'BEGE CLARO RAL 1015'),
  (220, 'VERDE STARA PBK112'),
  (282, 'CINZA EXECUTIVO'),
  (209, 'ROSA PINK 4010'),
  (269, 'VINHO RAL 3005'),
  (667, 'CINZA 7035'),
  (114, 'CORES PADRÃO'),
  (742, 'DOURADO PSW005'),
  (513, 'AÇO CORTEN WEG')
ON CONFLICT (pro_codigo) DO NOTHING;

/* === Totais de m² já existiam; agora criaremos campos ESPECÍFICOS da PINTURA === */
ALTER TABLE op
  ADD COLUMN IF NOT EXISTS m2_pintura_total_hdr     NUMERIC(18,3),
  ADD COLUMN IF NOT EXISTS m2_pintura_produzido_hdr NUMERIC(18,3),
  ADD COLUMN IF NOT EXISTS m2_pintura_saldo_hdr     NUMERIC(18,3);

/* === Marca por item se ele é item de PINTURA (derivado de cfg_pintura_prod) === */
ALTER TABLE op_item
  ADD COLUMN IF NOT EXISTS is_pintura BOOLEAN;

/* (Opcional) Índice útil quando formos consultar produtividade da pintura */
CREATE INDEX IF NOT EXISTS idx_item_is_pintura ON op_item (is_pintura) WHERE is_pintura IS TRUE;
