// frontend/js/pintura.js
// -------------------------------------------------------------
// Tela “Faltando Pintura” – 100% LEITURA (não altera Firebird)
// - Busca /ops/faltando-pintura do backend
// - Agrupa por cor e imprime lista com botão “Detalhes”
// - Mantém TUDO isolado (não mexe no restante do seu front)
// -------------------------------------------------------------

(() => {
  // ====== CONFIGURAÇÃO BÁSICA ======
  // Se já possui uma variável global com a URL do backend, substitua aqui.
  const API_BASE = 'http://127.0.0.1:8000';

  // Elementos criados no index.html (Partes 1 e 2)
  const elBtnPintura = document.getElementById('btnPintura');
  const elPane       = document.getElementById('pinturaPane');
  const elResumo     = document.getElementById('pinturaResumo');
  const elLista      = document.getElementById('pinturaLista');

  // ====== GUARD-CLAUSE ======
  // Se o HTML ainda não tem os elementos acima, não faz nada.
  if (!elBtnPintura || !elPane || !elResumo || !elLista) return;

  // ====== HELPERS ======
  /** Formata uma data ISO (YYYY-MM-DDTHH:mm:ss) para YYYY-MM-DD */
  function fmtDate(d) {
    if (!d) return '';
    // Aceita Date, string ISO, ou já YYYY-MM-DD
    try {
      if (d instanceof Date) return d.toISOString().slice(0, 10);
      const s = String(d);
      return s.length >= 10 ? s.slice(0, 10) : s;
    } catch {
      return String(d);
    }
  }

  /** Agrupa um array por chave calculada */
  function groupBy(arr, keyGetter) {
    const map = new Map();
    for (const it of arr) {
      const k = (keyGetter(it) ?? 'SEM PINTURA').trim();
      if (!map.has(k)) map.set(k, []);
      map.get(k).push(it);
    }
    return map;
  }

  /** Desabilita/habilita o botão enquanto carrega */
  function setLoading(loading) {
    elBtnPintura.disabled = !!loading;
    elBtnPintura.textContent = loading ? 'Carregando…' : 'Faltando Pintura';
  }

  // ====== RENDER ======
  function renderResumo(data, porCor) {
    // Linha com janela e modo utilizado pela API
    const header = `
      <div style="margin:4px 0;">
        <small>Janela: ${data.window?.from ?? '-'} → ${data.window?.to ?? '-'}
        • Campo: ${data.window?.field ?? '-'}
        • Modo: ${data.mode ?? '-'}</small>
      </div>`;

    // Chips com contagem por cor
    const chips = [...porCor.entries()]
      .sort((a, b) => b[1].length - a[1].length)
      .map(([cor, arr]) =>
        `<span style="display:inline-block;margin:3px 8px 3px 0;padding:2px 8px;border:1px solid #ddd;border-radius:12px;">
           <b>${cor}</b>: ${arr.length}
         </span>`
      )
      .join('');

    elResumo.innerHTML = header + (chips || '<i>Nenhuma OP pendente de pintura.</i>');
  }

  function renderLista(porCor) {
    let html = '';
    for (const [cor, arr] of porCor.entries()) {
      html += `
        <div style="margin-top:16px;border:1px solid #eee;padding:10px;border-radius:8px;">
          <h4 style="margin:0 0 8px 0;">${cor}</h4>
          <table style="width:100%;border-collapse:collapse;">
            <thead>
              <tr style="text-align:left;border-bottom:1px solid #ddd;">
                <th style="padding:6px;">OP</th>
                <th style="padding:6px;">Descrição</th>
                <th style="padding:6px;">Status</th>
                <th style="padding:6px;">%</th>
                <th style="padding:6px;">Ref. Data</th>
                <th style="padding:6px;">Ações</th>
              </tr>
            </thead>
            <tbody>
              ${arr.map(op => `
                <tr style="border-bottom:1px solid #f5f5f5;">
                  <td style="padding:6px;white-space:nowrap;">${op.op_numero}</td>
                  <td style="padding:6px;">${op.descricao || ''}</td>
                  <td style="padding:6px;">${op.status_nome || ''}</td>
                  <td style="padding:6px;">${Number(op.percent_concluido ?? 0).toFixed(2)}%</td>
                  <td style="padding:6px;">
                    ${fmtDate(op.dt_validade || op.dt_prev_inicio || op.dt_emissao)}
                  </td>
                  <td style="padding:6px;">
                    <button class="btn btn-op-detalhe" data-op="${op.op_id}">Detalhes</button>
                  </td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>`;
    }
    elLista.innerHTML = html || '';
  }

  function wireDetalhes() {
    // Abre /ops/{id} e mostra um resumo em alert (pode trocar por modal da sua UI)
    elLista.querySelectorAll('.btn-op-detalhe').forEach(btn => {
      btn.addEventListener('click', async (ev) => {
        const opId = ev.currentTarget.getAttribute('data-op');
        try {
          const r = await fetch(`${API_BASE}/ops/${opId}`);
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          const det = await r.json();
          const op = det.op || {};
          const ri = det.resumo_itens || {};
          alert(
            `OP ${op.op_numero}\n` +
            `Status: ${op.status_nome || '-'} • %: ${(op.percent_concluido ?? 0).toFixed(2)}%\n` +
            `Cor: ${op.cor_txt || '-'}\n` +
            `Itens: ${ri.itens ?? 0} | Produzidas: ${ri.qtd_produzidas ?? 0} | Saldo: ${ri.qtd_saldo ?? 0}`
          );
        } catch (err) {
          console.error(err);
          alert('Erro ao buscar detalhes da OP.');
        }
      });
    });
  }

  // ====== FETCH & FLOW ======
  async function carregarFaltandoPintura() {
    // Observação: estes parâmetros batem com o back já criado.
    const url = `${API_BASE}/ops/faltando-pintura?filial=1&date_field=validade&days_back=7&days_ahead=30`;

    setLoading(true);
    elPane.style.display = 'block';
    elResumo.textContent = 'Carregando...';
    elLista.innerHTML = '';

    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      const porCor = groupBy(data.items || [], x => x.cor_txt || 'SEM PINTURA');
      renderResumo(data, porCor);
      renderLista(porCor);
      wireDetalhes();
    } catch (err) {
      console.error(err);
      elResumo.innerHTML = `<span style="color:#b00020;">Falha ao carregar: ${String(err)}</span>`;
    } finally {
      setLoading(false);
    }
  }

  // ====== EVENTOS ======
  // Ao clicar no botão, abre o painel e carrega a lista
  elBtnPintura.addEventListener('click', () => {
    carregarFaltandoPintura();
  });

  // CSS mínimo para botão .btn (fallback, caso não exista no seu CSS)
  (function injectFallbackCss(){
    const css = `
      .btn { cursor:pointer; padding:6px 10px; border:1px solid #ccc; border-radius:6px; background:#fff; }
      .btn:hover { background:#f6f6f6; }
      #pinturaPane h3 { margin: 0 0 8px 0; }
      #pinturaPane table th, #pinturaPane table td { font-size: 14px; }
    `;
    const style = document.createElement('style');
    style.setAttribute('data-pintura-fallback', '1');
    style.textContent = css;
    document.head.appendChild(style);
  })();
})();
