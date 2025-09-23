-- 1) Quantas OPs temos no cabeçalho?
SELECT COUNT(*) AS ops_no_cabecalho FROM op;

-- 2) Quantas linhas de itens foram copiadas?
SELECT COUNT(*) AS linhas_de_itens FROM op_item;

-- 3) Amostra de itens
SELECT op_id, op_numero, pro_codigo, pro_desc, cor_codigo, cor_nome, qtd, qtd_saldo
FROM op_item
ORDER BY op_numero DESC, opd_id
LIMIT 30;

-- 4) Existem códigos de pintura cadastrados?
SELECT COUNT(*) AS codigos_de_pintura FROM cfg_pintura_prod;

-- 5) (Opcional) m² de pintura de uma OP específica (ajuste o op_id se quiser)
WITH paint AS (
  SELECT i.qtd, i.qtd_produzidas, i.qtd_saldo
  FROM op_item i
  JOIN cfg_pintura_prod cfg ON cfg.pro_codigo = i.pro_codigo
  WHERE i.op_id = 6372  -- << troque se quiser validar outra OP
)
SELECT 
  COALESCE(SUM(qtd),0)            AS m2_pintura_total,
  COALESCE(SUM(qtd_produzidas),0) AS m2_pintura_produzida,
  COALESCE(SUM(qtd_saldo),0)      AS m2_pintura_saldo
FROM paint;
