const API_BASE = "/api";

const DATA_SOURCES = [
  ["ibge", "IBGE Localidades", "📍"],
  ["cnpj", "CNPJ / Receita Federal", "🏢"],
  ["tse", "TSE Candidatos", "🗳️"],
  ["emendas_cgu", "Emendas CGU", "📋"],
  ["servidores_cgu", "Servidores CGU", "👔"],
  ["sancoes_cgu", "Sanções CGU", "⛔"],
  ["pncp", "PNCP Contratos", "📄"],
  ["pgfn", "PGFN Dívida Ativa", "💸"],
  ["camara", "Câmara CEAP", "🏛️"],
  ["bndes", "BNDES Empréstimos", "🏦"],
  ["senado", "Senado CEAP", "🏛️"],
];

const NAV_ITEMS = [
  { key: "busca", label: "Busca avançada", href: "/index.html", icon: "🔍" },
  { key: "grafo", label: "Explorador de grafo", href: "/grafo.html", icon: "🕸️" },
  { key: "corrupcao", label: "Padrões de corrupção", href: "/corrupcao.html", icon: "⚠️" },
  { key: "pipelines", label: "Status dos pipeline", href: "/pipelines.html", icon: "📡" },
];

const LABEL_COLORS = {
  Pessoa: "#00ff94",
  Empresa: "#00e5ff",
  Parlamentar: "#ffd60a",
  Servidor: "#ff6b35",
  Sancao: "#ff3860",
  Emenda: "#b48ead",
  Partido: "#64748b",
  Municipio: "#94a3b8",
  Estado: "#94a3b8",
  Partner: "#38bdf8",
  default: "#475569",
};

function $(selector) {
  return document.querySelector(selector);
}

function fmtNumber(value) {
  if (value === null || value === undefined || value === "") return "-";
  return Number(value).toLocaleString("pt-BR");
}

function fmtCurrency(value) {
  if (value === null || value === undefined || value === "") return "-";
  return Number(value).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function fmtDate(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleDateString("pt-BR");
}

function onlyDigits(value) {
  return String(value || "").replace(/\D/g, "");
}

function maskCPF(value) {
  const digits = onlyDigits(value);
  if (!digits) return "-";
  const s = digits.padStart(11, "0");
  return `${s.slice(0, 3)}.${s.slice(3, 6)}.${s.slice(6, 9)}-${s.slice(9)}`;
}

function maskCNPJ(value) {
  const digits = onlyDigits(value);
  if (!digits) return "-";
  const s = digits.padStart(14, "0");
  return `${s.slice(0, 2)}.${s.slice(2, 5)}.${s.slice(5, 8)}/${s.slice(8, 12)}-${s.slice(12)}`;
}

function labelBadge(label) {
  const color = LABEL_COLORS[label] || LABEL_COLORS.default;
  return `<span class="badge" style="color:${color}; background:${color}18">${label}</span>`;
}

async function apiFetch(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

function pageTitleFromKey(key) {
  const item = NAV_ITEMS.find((entry) => entry.key === key);
  return item ? item.label : "DABERTO";
}

function buildShell(pageKey) {
  const app = $("#app");
  app.innerHTML = `
    <div class="layout">
      <aside class="sidebar">
        <div class="logo">
          <a href="/index.html">
            <div class="logo-mark">D<span>ABERTO</span></div>
            <div class="logo-sub">Inteligência Cívica</div>
          </a>
        </div>
        <nav class="nav">
          <section class="nav-section">
            <div class="nav-label">Navegação</div>
            ${NAV_ITEMS.map((item) => `
              <a class="nav-item ${item.key === pageKey ? "active" : ""}" href="${item.href}">
                <span>${item.icon}</span>
                <span>${item.label}</span>
              </a>
            `).join("")}
          </section>
          <section class="nav-section">
            <div class="nav-label">Bases de dados</div>
            ${DATA_SOURCES.map(([key, label, icon]) => `
              <div class="nav-item data-source">
                <span>${icon}</span>
                <span>${label}</span>
              </div>
            `).join("")}
          </section>
        </nav>
        <div class="sidebar-footer">
          <div>frontend: localhost:8080</div>
          <div>api: localhost:8000</div>
          <div>neo4j: localhost:7687</div>
        </div>
      </aside>
      <main class="main">
        <header class="topbar">
          <div class="topbar-title">${pageTitleFromKey(pageKey)}</div>
          <div class="topbar-actions">
            <a class="topbar-link" href="/api/docs" target="_blank" rel="noreferrer">API Docs</a>
          </div>
        </header>
        <div class="content" id="page-content"></div>
      </main>
    </div>
  `;
}

function renderHeader({ label, title, subtitle }) {
  return `
    <div class="page-header">
      <div class="page-label">${label}</div>
      <div class="page-title">${title}</div>
      <div class="page-subtitle">${subtitle}</div>
    </div>
  `;
}

function resultIdentity(item) {
  if (item.label === "Pessoa") return maskCPF(item.cpf || item.id);
  if (item.label === "Empresa") return maskCNPJ(item.cnpj_basico || item.id);
  return item.id || "-";
}

function buildProfileUrl(label, id) {
  return `/perfil.html?tipo=${encodeURIComponent(label)}&id=${encodeURIComponent(id)}`;
}

function mountSearchPage() {
  const root = $("#page-content");
  root.innerHTML = `
    ${renderHeader({
      label: "Busca avancada",
      title: "Pesquisa operacional no grafo",
      subtitle: "Localize pessoas, empresas, parlamentares e outros nós do grafo, ainda que por similaridade.",
    })}
    <section class="card">
      <div class="toolbar">
        <div class="field" style="grid-column: span 8;">
          <label>Termo</label>
          <input id="search-query" placeholder="CPF, CNPJ, nome, codigo de emenda, partido..." />
        </div>
        <div class="field" style="grid-column: span 2;">
          <label>Tipo</label>
          <select id="search-label">
            <option value="">Todos</option>
            <option>Pessoa</option>
            <option>Empresa</option>
            <option>Parlamentar</option>
            <option>Municipio</option>
            <option>Estado</option>
          </select>
        </div>
        <div class="field" style="grid-column: span 2;">
          <label>&nbsp;</label>
          <button class="button" id="search-button">Buscar</button>
        </div>
      </div>
    </section>
    <section class="card">
      <div class="card-title">Resultados</div>
      <div id="search-feedback" class="muted">Digite pelo menos 3 caracteres para consultar o indice fulltext.</div>
      <div id="search-results" class="results-list"></div>
    </section>
  `;

  async function runSearch() {
    const q = $("#search-query").value.trim();
    const label = $("#search-label").value;
    const feedback = $("#search-feedback");
    const container = $("#search-results");
    container.innerHTML = "";

    if (q.length < 3) {
      feedback.textContent = "Digite pelo menos 3 caracteres.";
      return;
    }

    feedback.textContent = "Consultando...";
    try {
      const data = await apiFetch(`/search?q=${encodeURIComponent(q)}&limit=30`);
      const items = label ? data.items.filter((item) => item.label === label) : data.items;
      feedback.textContent = `${items.length} resultado(s)`;

      if (!items.length) {
        container.innerHTML = `<div class="empty-state">Nenhum resultado para esse filtro.</div>`;
        return;
      }

      container.innerHTML = items.map((item) => `
        <a class="result-item" href="${buildProfileUrl(item.label, item.id)}">
          <div>${labelBadge(item.label)}</div>
          <div>
            <div class="result-title">${item.nome || item.razao_social || item.id}</div>
            <div class="muted mono" style="margin-top:4px;">${resultIdentity(item)}</div>
          </div>
          <div class="muted mono">similaridade ${Number(item.score || 0).toFixed(2)}</div>
        </a>
      `).join("");
    } catch (error) {
      feedback.textContent = "Nao foi possivel consultar a API.";
      container.innerHTML = `<div class="error-state">${error.message}</div>`;
    }
  }

  $("#search-button").addEventListener("click", runSearch);
  $("#search-query").addEventListener("keydown", (event) => {
    if (event.key === "Enter") runSearch();
  });
}

function makeGraphShell() {
  return `
    <div class="graph-shell">
      <div id="graph-canvas" class="graph-canvas"></div>
      <div id="graph-canvas-3d" class="graph-canvas hidden"></div>
      <div class="graph-meta">
        <div class="muted" id="graph-meta">Clique em um no para expandir mais conexoes.</div>
      </div>
    </div>
  `;
}

async function renderGraph2D(cytoscape, graph, onNodeTap) {
  const elements = [
    ...graph.nodes.map((node) => {
      const keyPart = node.uid ? node.uid.split(":")[1] || node.uid : node.uid;
      const label = node.nome || node.razao_social || node.nome_autor || keyPart;
      return {
        data: {
          id: node.uid,
          label: label,
          nodeLabel: node.label,
          color: LABEL_COLORS[node.label] || LABEL_COLORS.default,
        },
      };
    }),
    ...graph.edges.map((edge, index) => ({
      data: {
        id: `${edge.source}-${edge.target}-${edge.type}-${index}`,
        source: edge.source,
        target: edge.target,
        edgeLabel: edge.type,
      },
    })),
  ];

  const cy = cytoscape({
    container: $("#graph-canvas"),
    elements,
    layout: {
      name: "cose",
      animate: false,
      fit: true,
      padding: 60,
      spacingFactor: 1.35,
      nodeRepulsion: 120000,
      idealEdgeLength: 140,
      edgeElasticity: 80,
      gravity: 0.2,
    },
    style: [
      {
        selector: "node",
        style: {
          label: "data(label)",
          "background-color": "data(color)",
          color: "#dbeafe",
          "font-size": 9,
          "font-weight": 700,
          "text-wrap": "wrap",
          "text-max-width": 84,
          width: 24,
          height: 24,
          "border-width": 1,
          "border-color": "#081017",
        },
      },
       {
         selector: "edge",
         style: {
           width: 1.5,
           "line-color": "#4b5563",
           "curve-style": "bezier",
           "target-arrow-shape": "triangle",
           "target-arrow-color": "#4b5563",
           label: "data(edgeLabel)",
           "font-size": 8,
           "text-rotation": "autorotate",
           "text-margin-y": -3,
           color: "#9ca3af",
         },
       },
    ],
  });

  cy.on("tap", "node", (event) => {
    const id = event.target.id();
    const [label, rawId] = id.split(":");
    if (label && rawId) onNodeTap(label, rawId, event.target.data(), event);
  });

  return cy;
}

function mountGraphPage(cytoscape) {
  const params = new URLSearchParams(window.location.search);
  const initialLabel = params.get("label");
  const initialId = params.get("id");
  const initialMode = params.get("mode") || "2d";
  const root = $("#page-content");
  root.innerHTML = `
    ${renderHeader({
      label: "Explorador de grafo",
      title: "Expansão manual de relacionamentos",
      subtitle: "Entre com a entidade raiz e navegue pelo grafo sem passar por uma landing page. A selecao aqui foca em operacao e leitura.",
    })}
    <section class="card">
      <div class="toolbar">
        <div class="field" style="grid-column: span 3;">
          <label>Label</label>
          <select id="graph-label">
            <option>Empresa</option>
            <option>Pessoa</option>
            <option>Parlamentar</option>
            <option>Municipio</option>
            <option>Estado</option>
          </select>
        </div>
        <div class="field" style="grid-column: span 4;">
          <label>Identificador</label>
          <input id="graph-id" placeholder="CPF, CNPJ basico, id parlamentar, UF..." />
        </div>
        <div class="field" style="grid-column: span 2;">
          <label>Visualização</label>
          <div class="mode-toggle">
            <button type="button" class="mode-option active" data-mode="2d">2D</button>
            <button type="button" class="mode-option" data-mode="3d">3D</button>
          </div>
        </div>
        <div class="field" style="grid-column: span 3;">
          <label>&nbsp;</label>
          <button class="button" id="graph-load">Expandir</button>
          <button class="button" id="export-pdf-btn" style="background-color: #dc2626; margin-left: 8px;">Exportar PDF</button>
        </div>
      </div>
    </section>>
    <section id="graph-layout" class="split graph-layout">
      <div class="card">${makeGraphShell()}</div>
      <div class="stack">
        <section class="card">
          <div class="card-title">Seleção atual</div>
          <div id="graph-selection" class="muted">Nenhum nó selecionado.</div>
        </section>
        <section class="card">
          <div class="card-title">Estatísticas</div>
          <div id="graph-stats" class="pill-row"></div>
        </section>
        <section class="card">
          <div class="card-title">Legenda</div>
          <div id="graph-legend" class="graph-legend"></div>
        </section>
      </div>
    </section>
  `;

  let currentGraph = null;
  let currentCy = null;
  let currentGraph3D = null;
  let graphMode = initialMode === "3d" ? "3d" : "2d";

  function syncModeButtons() {
    document.querySelectorAll(".mode-option").forEach((button) => {
      button.classList.toggle("active", button.dataset.mode === graphMode);
    });
    $("#graph-layout")?.classList.toggle("mode-3d", graphMode === "3d");
  }

  function destroy3D() {
    const container = $("#graph-canvas-3d");
    if (container) {
      container.innerHTML = "";
    }
    currentGraph3D = null;
  }

  async function renderGraph3D(graph, onNodeTap) {
    const container = $("#graph-canvas-3d");
    if (!window.ForceGraph3D) {
      container.innerHTML = `<div class="error-state">Modo 3D indisponivel neste navegador.</div>`;
      return;
    }

    destroy3D();

     const graphData = {
       nodes: (graph.nodes || []).map((node) => {
         const keyPart = node.uid ? node.uid.split(":")[1] || node.uid : node.uid;
         const name = node.nome || node.razao_social || node.nome_autor || keyPart;
         return {
           id: node.uid,
           uid: node.uid,
           label: node.label,
           name: name,
           color: LABEL_COLORS[node.label] || LABEL_COLORS.default,
           val: 5,
         };
       }),
      links: (graph.edges || []).map((edge, index) => ({
        id: `${edge.source}-${edge.target}-${edge.type}-${index}`,
        source: edge.source,
        target: edge.target,
        type: edge.type,
      })),
    };

    currentGraph3D = ForceGraph3D()(container)
      .backgroundColor("#090b0d")
      .width(container.clientWidth || container.offsetWidth || 800)
      .height(container.clientHeight || container.offsetHeight || 520)
      .graphData(graphData)
      .nodeColor("color")
      .nodeVal("val")
      .linkColor(() => "#4b5563")
      .linkOpacity(0.55)
      .linkWidth(1)
      .nodeLabel((node) => `${node.label}: ${node.name}`)
      .onNodeClick((node) => {
        const distance = 90;
        const magnitude = Math.hypot(node.x || 0, node.y || 0, node.z || 0) || 1;
        const distRatio = 1 + distance / magnitude;

        currentGraph3D.cameraPosition(
          {
            x: (node.x || 0) * distRatio,
            y: (node.y || 0) * distRatio,
            z: (node.z || 0) * distRatio,
          },
          { x: node.x || 0, y: node.y || 0, z: node.z || 0 },
          900
        );

         const [label, rawId] = String(node.uid).split(":");
         if (label && rawId) {
           setTimeout(() => {
             onNodeTap(label, rawId, { nodeLabel: node.label, label: node.name });
           }, 220);
         }
      });

    if (window.SpriteText) {
      currentGraph3D.nodeThreeObject((node) => {
        const sprite = new SpriteText(node.name);
        sprite.color = node.color || "#dbeafe";
        sprite.textHeight = 2.2;
        return sprite;
      });
    }

    setTimeout(() => {
      if (currentGraph3D) {
        currentGraph3D.zoomToFit(900, 120);
        currentGraph3D.cameraPosition({ x: 0, y: 0, z: 220 });
      }
    }, 700);
  }

  function mergeGraphData(baseGraph, incomingGraph) {
    if (!baseGraph) {
      return {
        nodes: [...(incomingGraph.nodes || [])],
        edges: [...(incomingGraph.edges || [])],
        meta: incomingGraph.meta || {},
      };
    }

    const nodesByUid = new Map((baseGraph.nodes || []).map((node) => [node.uid, node]));
    for (const node of incomingGraph.nodes || []) {
      nodesByUid.set(node.uid, { ...(nodesByUid.get(node.uid) || {}), ...node });
    }

    const edgesByKey = new Map(
      (baseGraph.edges || []).map((edge) => [`${edge.source}|${edge.target}|${edge.type}`, edge])
    );
    for (const edge of incomingGraph.edges || []) {
      edgesByKey.set(`${edge.source}|${edge.target}|${edge.type}`, edge);
    }

    return {
      nodes: Array.from(nodesByUid.values()),
      edges: Array.from(edgesByKey.values()),
      meta: incomingGraph.meta || baseGraph.meta || {},
    };
  }

  async function redrawGraph() {
    $("#graph-canvas").classList.toggle("hidden", graphMode !== "2d");
    $("#graph-canvas-3d").classList.toggle("hidden", graphMode !== "3d");

    const onNodeTap = async (nextLabel, nextId, data) => {
      $("#graph-selection").innerHTML = `${labelBadge(data.nodeLabel)}<div style="margin-top:10px;" class="result-title">${data.label}</div><div class="muted mono" style="margin-top:6px;">${nextId}</div>`;
      $("#graph-meta").textContent = `Expandindo conexões de ${data.label}...`;

      try {
        const expanded = await apiFetch(`/graph/expand?label=${encodeURIComponent(nextLabel)}&id=${encodeURIComponent(nextId)}&hops=1&max_nodes=80`);
        currentGraph = mergeGraphData(currentGraph, expanded);
        $("#graph-stats").innerHTML = `
          <span class="pill">${currentGraph.nodes.length} nós</span>
          <span class="pill">${currentGraph.edges.length} arestas</span>
          <span class="pill">ultimo grau ${expanded.meta.degree}</span>
          <span class="pill">expansão acumulada</span>
        `;
        await redrawGraph();
        $("#graph-meta").textContent = `Conexões de ${data.label} adicionadas ao grafo.`;
      } catch (error) {
        $("#graph-meta").textContent = `Falha ao expandir ${data.label}.`;
      }
    };

    if (graphMode === "2d") {
      destroy3D();
      if (currentCy) currentCy.destroy();
      currentCy = await renderGraph2D(cytoscape, currentGraph, onNodeTap);
    } else {
      if (currentCy) {
        currentCy.destroy();
        currentCy = null;
      }
      await renderGraph3D(currentGraph, onNodeTap);
    }
  }

  $("#graph-legend").innerHTML = [
    ["Verde", "Pessoa", LABEL_COLORS.Pessoa],
    ["Azul", "Empresa", LABEL_COLORS.Empresa],
    ["Amarelo", "Parlamentar", LABEL_COLORS.Parlamentar],
    ["Laranja", "Servidor", LABEL_COLORS.Servidor],
    ["Vermelho", "Sancao", LABEL_COLORS.Sancao],
    ["Roxo", "Emenda", LABEL_COLORS.Emenda],
    ["Cinza", "Municipio", LABEL_COLORS.Municipio],
    ["Cinza claro", "Estado", LABEL_COLORS.Estado],
    ["Azul claro", "Partner", LABEL_COLORS.Partner],
  ].map(([colorName, label, color]) => `
    <div class="graph-legend-item">
      <span class="graph-legend-dot" style="background:${color};"></span>
      <span>${colorName} - ${label}</span>
    </div>
  `).join("");

  async function resolveGraphTarget(label, rawValue) {
    const term = rawValue.trim();

    try {
      await apiFetch(`/graph/expand?label=${encodeURIComponent(label)}&id=${encodeURIComponent(term)}&hops=1&max_nodes=1`);
      return { label, id: term, resolvedBy: "exact" };
    } catch (error) {
      if (!String(error.message || "").includes("404")) {
        throw error;
      }
    }

    const search = await apiFetch(`/search?q=${encodeURIComponent(term)}&limit=20`);
    const exactLabelMatch = (search.items || []).find((item) => item.label === label && item.id);
    const fallbackMatch = (search.items || []).find((item) => item.id);
    const chosen = exactLabelMatch || fallbackMatch;

    if (!chosen) {
      throw new Error(`Nenhum resultado encontrado para "${term}".`);
    }

    return {
      label: chosen.label,
      id: chosen.id,
      resolvedBy: "search",
      nome: chosen.nome,
      requestedLabel: label,
    };
  }

    async function loadGraph(label, id, hops) {
      $("#graph-meta").textContent = "Carregando conexoes...";
      try {
        const resolved = await resolveGraphTarget(label, id);
        const effectiveLabel = resolved.label;
        const effectiveId = resolved.id;

        if (resolved.resolvedBy === "search") {
          $("#graph-label").value = effectiveLabel;
          $("#graph-id").value = effectiveId;
          $("#graph-meta").textContent =
            resolved.requestedLabel === effectiveLabel
              ? `Termo resolvido por busca: ${resolved.nome || effectiveId}.`
              : `Termo resolvido por busca: ${resolved.nome || effectiveId} (${effectiveLabel}).`;
        }

        currentGraph = await apiFetch(`/graph/expand?label=${encodeURIComponent(effectiveLabel)}&id=${encodeURIComponent(effectiveId)}&hops=${hops}&max_nodes=120`);
        $("#graph-selection").innerHTML = `${labelBadge(effectiveLabel)}<div style="margin-top:10px;" class="mono">${effectiveId}</div>`;
        $("#graph-stats").innerHTML = `
          <span class="pill">${currentGraph.nodes.length} nós</span>
          <span class="pill">${currentGraph.edges.length} arestas</span>
          <span class="pill">grau raiz ${currentGraph.meta.degree}</span>
          <span class="pill">${currentGraph.meta.is_supernode ? "Limite de super nó ativo" : "expansão completa"}</span>
        `;

        await redrawGraph();
        $("#graph-meta").textContent = "Clique em um nó para adicionar novas conexões ao grafo atual.";
      } catch (error) {
        $("#graph-meta").textContent = "Nao foi possivel carregar o grafo.";
        $("#graph-selection").innerHTML = `<div class="error-state">${error.message}</div>`;
      }
    }

    async function exportGraphToPDF() {
      const btn = $("#export-pdf-btn");
      const originalText = btn.textContent;
      btn.textContent = "Gerando...";
      btn.disabled = true;

      try {
        if (!window.jspdf) {
          await loadScript("https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js");
        }
        if (!window.html2canvas) {
          await loadScript("https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js");
        }

        const { jsPDF } = window.jspdf;

        const graphCanvas = $("#graph-canvas");
        const canvas = await window.html2canvas(graphCanvas, {
          backgroundColor: "#090b0d",
          scale: 2,
        });

        const imgData = canvas.toDataURL("image/png");
        const pdf = new jsPDF("l", "mm", "a4");
        const pageWidth = pdf.internal.pageSize.getWidth();
        const pageHeight = pdf.internal.pageSize.getHeight();

        pdf.addImage(imgData, "PNG", 0, 0, pageWidth, pageHeight);

        const effectiveLabel = $("#graph-label").value;
        const effectiveId = $("#graph-id").value;
        const stats = `${currentGraph?.nodes?.length || 0} nós, ${currentGraph?.edges?.length || 0} arestas`;
        
        pdf.setFontSize(9);
        pdf.setTextColor(100);
        pdf.text(`Grafo: ${effectiveLabel} | ID: ${effectiveId} | ${stats}`, 10, pageHeight - 20);
        pdf.text(`Fonte: DABERTO - dados abertos | ${new Date().toLocaleDateString("pt-BR")}`, 10, pageHeight - 12);

        pdf.addPage();
        pdf.setFontSize(12);
        pdf.setTextColor(0);
        pdf.text("Descrição dos dados e origem", 10, 15);

        pdf.setFontSize(10);
        pdf.setTextColor(80);
        const description = [
          "DABERTO - Inteligência Cívica",
          "Infraestrutura open-source que cruza bases públicas brasileiras em grafo Neo4j.",
          "",
          "Bases de dados incluídas:",
          "• IBGE - Localidades (municípios, estados, regiões)",
          "• CNPJ / Receita Federal - Empresas e sócios",
          "• TSE - Candidatos, partidos e eleições",
          "• Emendas CGU - Emendas parlamentares",
          "• Servidores CGU - Servidores públicos",
          "• Sanções CGU - Penalidades e sanções",
          "• PNCP - Contratos da administração pública",
          "• PGFN - Dívida ativa",
          "• Câmara CEAP - Cotas parlamentares",
          "• BNDES - Empréstimos",
          "• Senado CEAP - Cotas senatoriais",
          "",
          "Origem dos dados: dados.gov.br, portals de transparência, TSE, Receita Federal.",
        ];

        let y = 20;
        for (const line of description) {
          pdf.text(line, 10, y);
          y += 7;
        }

        pdf.save(`grafo-${effectiveLabel}-${effectiveId}.pdf`);
      } catch (error) {
        alert("Erro ao gerar PDF: " + error.message);
      } finally {
        btn.textContent = originalText;
        btn.disabled = false;
      }
    }

    function loadScript(src) {
      return new Promise((resolve, reject) => {
        if (document.querySelector(`script[src="${src}"]`)) {
          resolve();
          return;
        }
        const script = document.createElement("script");
        script.src = src;
        script.onload = resolve;
        script.onerror = reject;
        document.head.appendChild(script);
      });
    }

    $("#graph-load").addEventListener("click", () => {
      loadGraph($("#graph-label").value, $("#graph-id").value.trim(), 1);
    });

    $("#export-pdf-btn").addEventListener("click", exportGraphToPDF);

    document.querySelectorAll(".mode-option").forEach((button) => {
      button.addEventListener("click", async () => {
        const nextMode = button.dataset.mode;
        if (nextMode === graphMode) return;
        graphMode = nextMode;
        syncModeButtons();
        if (currentGraph) {
          await redrawGraph();
        }
      });
    });

    window.addEventListener("resize", () => {
      if (graphMode === "3d" && currentGraph3D) {
        const container = $("#graph-canvas-3d");
        currentGraph3D.width(container.clientWidth || container.offsetWidth || 800);
        currentGraph3D.height(container.clientHeight || container.offsetHeight || 520);
        setTimeout(() => {
          if (currentGraph3D) {
            currentGraph3D.zoomToFit(700, 120);
          }
        }, 120);
      }
    });

    syncModeButtons();

    if (initialLabel && initialId) {
      $("#graph-label").value = initialLabel;
      $("#graph-id").value = initialId;
      loadGraph(initialLabel, initialId, 1);
    }
  }

function normalizeProfileValue(tipo, id, payload) {
  if (tipo === "Pessoa") return payload.pessoa?.nome || maskCPF(id);
  if (tipo === "Empresa") return payload.empresa?.razao_social || maskCNPJ(id);
  if (tipo === "Parlamentar") return payload.parlamentar?.nome_autor || id;
  return id;
}

function mountProfilePage(cytoscape) {
  const params = new URLSearchParams(window.location.search);
  const tipo = params.get("tipo");
  const id = params.get("id");
  const root = $("#page-content");

  root.innerHTML = `
    ${renderHeader({
      label: "Perfil",
      title: "Detalhamento da entidade",
      subtitle: "Dados da entidade selecionada de forma mais simples e textual.",
    })}
    <div id="profile-root" class="stack">
      <div class="loading-state">Carregando perfil...</div>
    </div>
  `;

  if (!tipo || !id) {
    $("#profile-root").innerHTML = `<div class="error-state">Parâmetros tipo e id são obrigatórios.</div>`;
    return;
  }

  const endpointMap = {
    Pessoa: `/pessoa/${encodeURIComponent(id)}`,
    Empresa: `/empresa/${encodeURIComponent(id)}`,
    Parlamentar: `/parlamentar/${encodeURIComponent(id)}`,
  };

  const endpoint = endpointMap[tipo];
  if (!endpoint) {
    $("#profile-root").innerHTML = `<div class="error-state">Tipo ${tipo} ainda não tem tela estruturada.</div>`;
    return;
  }

  async function loadProfile() {
    try {
      const payload = await apiFetch(endpoint);
      const title = normalizeProfileValue(tipo, id, payload);
      document.title = `${title} | DABERTO`;

      const stats = [];
      if (tipo === "Pessoa") {
        stats.push(["sociedades", payload.socios?.length || 0]);
        stats.push(["candidaturas", payload.candidaturas?.length || 0]);
        stats.push(["duplicatas", payload.duplicatas?.length || 0]);
      }
      if (tipo === "Empresa") {
        stats.push(["socios PF", payload.socios_pf?.length || 0]);
        stats.push(["contratos", payload.contratos?.length || 0]);
        stats.push(["sancoes", payload.sancoes?.length || 0]);
      }
      if (tipo === "Parlamentar") {
        stats.push(["emendas", payload.emendas?.length || 0]);
        stats.push(["doadores", payload.doadores?.length || 0]);
        stats.push(["empresas com sancao", payload.empresas_com_sancao?.length || 0]);
      }

      $("#profile-root").innerHTML = `
        <section class="card">
          <div class="page-title" style="font-size:28px;">${title}</div>
          <div class="page-subtitle">${tipo} | identificador ${id}</div>
          <div class="grid-3" style="margin-top:20px;">
            ${stats.map(([label, value]) => `
              <div class="stat">
                <div class="stat-label">${label}</div>
                <div class="stat-value">${fmtNumber(value)}</div>
              </div>
            `).join("")}
          </div>
          <div class="link-row" style="margin-top:18px;">
            <a class="button secondary" href="/grafo.html?label=${encodeURIComponent(tipo)}&id=${encodeURIComponent(id)}&hops=1">Explorador de grafo</a>
            ${tipo === "Empresa" ? `<a class="button subtle" href="/corrupcao.html?cnpj=${encodeURIComponent(id)}">Padrões de corrupção</a>` : ""}
          </div>
        </section>
        <section class="split">
          <div class="card">${makeGraphShell()}</div>
          <div class="stack" id="profile-side"></div>
        </section>
        <section class="card" id="profile-sections"></section>
      `;

      const side = $("#profile-side");
      const sections = $("#profile-sections");

      side.innerHTML = `
        <section class="card">
          <div class="card-title">Dados principais</div>
          <div class="detail-list">${renderPrimaryDetails(tipo, payload)}</div>
        </section>
      `;

      if (tipo === "Empresa") {
        sections.innerHTML = renderEmpresaSections(payload);
      } else if (tipo === "Pessoa") {
        sections.innerHTML = renderPessoaSections(payload);
      } else if (tipo === "Parlamentar") {
        sections.innerHTML = renderParlamentarSections(payload);
      }

      // ── Popup de nó ──────────────────────────────────────────────────────
      if (!document.getElementById("node-popup")) {
        const popupEl = document.createElement("div");
        popupEl.id = "node-popup";
        popupEl.innerHTML = `
          <button id="node-popup-close" title="Fechar">✕</button>
          <div id="node-popup-body"></div>
        `;
        document.body.appendChild(popupEl);

        if (!document.getElementById("node-popup-style")) {
          const style = document.createElement("style");
          style.id = "node-popup-style";
          style.textContent = `
            #node-popup {
              display: none;
              position: fixed;
              z-index: 9999;
              min-width: 260px;
              max-width: 340px;
              background: #0f1923;
              border: 1px solid #1e3044;
              border-radius: 10px;
              box-shadow: 0 8px 32px rgba(0,0,0,0.55);
              padding: 18px 18px 14px 18px;
              font-family: inherit;
              pointer-events: auto;
            }
            #node-popup.visible { display: block; }
            #node-popup-close {
              position: absolute;
              top: 10px; right: 12px;
              background: none;
              border: none;
              color: #64748b;
              font-size: 14px;
              cursor: pointer;
              line-height: 1;
              padding: 2px 6px;
              border-radius: 4px;
              transition: color .15s;
            }
            #node-popup-close:hover { color: #e2e8f0; }
            #node-popup-body .popup-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .08em; margin-bottom: 6px; }
            #node-popup-body .popup-title { font-size: 15px; font-weight: 700; color: #e2e8f0; line-height: 1.35; margin-bottom: 10px; }
            #node-popup-body .popup-rows { display: flex; flex-direction: column; gap: 5px; margin-bottom: 12px; }
            #node-popup-body .popup-row { display: flex; justify-content: space-between; gap: 8px; font-size: 12px; }
            #node-popup-body .popup-row-key { color: #64748b; white-space: nowrap; }
            #node-popup-body .popup-row-val { color: #cbd5e1; text-align: right; font-family: monospace; word-break: break-all; }
            #node-popup-body .popup-actions { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 4px; }
            #node-popup-body .popup-btn {
              display: inline-block;
              font-size: 12px;
              font-weight: 600;
              padding: 5px 13px;
              border-radius: 6px;
              text-decoration: none;
              cursor: pointer;
              transition: opacity .15s;
            }
            #node-popup-body .popup-btn:hover { opacity: .8; }
            #node-popup-body .popup-btn-primary { background: #00ff94; color: #0a1628; }
            #node-popup-body .popup-btn-secondary { background: #1e3044; color: #94a3b8; }
            #node-popup-body .popup-loading { color: #64748b; font-size: 13px; }
          `;
          document.head.appendChild(style);
        }

        document.getElementById("node-popup-close").addEventListener("click", () => {
          document.getElementById("node-popup").classList.remove("visible");
        });

        document.addEventListener("keydown", (e) => {
          if (e.key === "Escape") document.getElementById("node-popup")?.classList.remove("visible");
        });
      }

      function showNodePopup(nodeLabel, nodeId, nodeData, cyEvent) {
        const popup = document.getElementById("node-popup");
        const body = document.getElementById("node-popup-body");
        const color = LABEL_COLORS[nodeLabel] || LABEL_COLORS.default;

        body.innerHTML = `
          <div class="popup-label" style="color:${color}">${nodeLabel}</div>
          <div class="popup-title">${nodeData.label || nodeId}</div>
          <div class="popup-loading">Carregando dados…</div>
        `;
        popup.classList.add("visible");

        // Posiciona perto do clique (mas não sai da tela)
        const vw = window.innerWidth, vh = window.innerHeight;
        let x = (cyEvent?.originalEvent?.clientX ?? vw / 2) + 14;
        let y = (cyEvent?.originalEvent?.clientY ?? vh / 2) - 20;
        if (x + 360 > vw) x = vw - 360;
        if (y + 360 > vh) y = vh - 360;
        if (y < 10) y = 10;
        popup.style.left = x + "px";
        popup.style.top = y + "px";

        // Busca dados da entidade na API, se houver endpoint
        const endpointMap = {
          Pessoa: `/pessoa/${encodeURIComponent(nodeId)}`,
          Empresa: `/empresa/${encodeURIComponent(nodeId)}`,
          Parlamentar: `/parlamentar/${encodeURIComponent(nodeId)}`,
          ContratoComprasNet: `/contrato/${encodeURIComponent(nodeId)}`,
          Sancao: `/sancao/${encodeURIComponent(nodeId)}`,
        };
        const ep = endpointMap[nodeLabel];

        const profileUrl = buildProfileUrl(nodeLabel, nodeId);
        const grafUrl = `/grafo.html?label=${encodeURIComponent(nodeLabel)}&id=${encodeURIComponent(nodeId)}&hops=1`;

        if (!ep) {
          // Nós sem endpoint dedicado (Municipio, Estado, Emenda, Sancao, etc.)
          body.innerHTML = `
            <div class="popup-label" style="color:${color}">${nodeLabel}</div>
            <div class="popup-title">${nodeData.label || nodeId}</div>
            <div class="popup-rows">
              <div class="popup-row"><span class="popup-row-key">ID</span><span class="popup-row-val">${nodeId}</span></div>
            </div>
            <div class="popup-actions">
              <a class="popup-btn popup-btn-secondary" href="${grafUrl}">Ver no grafo</a>
            </div>
          `;
          return;
        }

        apiFetch(ep).then((payload) => {
          let rows = [];
          let titleText = nodeData.label || nodeId;
          let actions = `
            <a class="popup-btn popup-btn-primary" href="${profileUrl}">Ver perfil</a>
            <a class="popup-btn popup-btn-secondary" href="${grafUrl}">Ver no grafo</a>
          `;

          if (nodeLabel === "Pessoa") {
            const p = payload.pessoa || {};
            titleText = p.nome || titleText;
            rows = [
              ["CPF", maskCPF(p.cpf || nodeId)],
              ["Nascimento", fmtDate(p.dt_nascimento)],
              ["Sociedades", payload.socios?.length ?? "-"],
              ["Candidaturas", payload.candidaturas?.length ?? "-"],
              ["Parl. vinculado", payload.parlamentar?.nome_parlamentar || "-"],
            ];
          } else if (nodeLabel === "Empresa") {
            const e = payload.empresa || {};
            titleText = e.razao_social || titleText;
            rows = [
              ["CNPJ", maskCNPJ(e.cnpj_basico || nodeId)],
              ["UF", e.uf || "-"],
              ["Situação", e.situacao_cadastral || "-"],
              ["Sócios PF", payload.socios_pf?.length ?? "-"],
              ["Sanções", payload.sancoes?.length ?? "-"],
            ];
            if (payload.contratos && payload.contratos.length > 0) {
              payload.contratos.slice(0, 3).forEach(c => {
                rows.push(["Contrato", c.objeto + " (" + fmtCurrency(c.valor) + ")"]);
              });
              if (payload.contratos.length > 3) {
                rows.push(["", "e mais " + (payload.contratos.length - 3) + " contratos"]);
              }
            } else {
              rows.push(["Contratos", "0"]);
            }
            actions += `<a class="popup-btn popup-btn-secondary" href="/corrupcao.html?cnpj=${encodeURIComponent(nodeId)}" style="margin-top:4px;">Padrões de risco</a>`;
          } else if (nodeLabel === "Parlamentar") {
            const parl = payload.parlamentar || {};
            titleText = parl.nome_autor || titleText;
            rows = [
              ["CPF", maskCPF(parl.cpf)],
              ["Partido", parl.sigla_partido || parl.partido || "-"],
              ["UF", parl.uf || "-"],
              ["Emendas", payload.emendas?.length ?? "-"],
              ["Doadores", payload.doadores?.length ?? "-"],
            ];
          } else if (nodeLabel === "ContratoComprasNet") {
            const c = payload.contrato || {};
            titleText = c.objeto || titleText;
            rows = [
              ["Número", c.numero_contrato || "-"],
              ["Ano", c.ano_contrato || "-"],
              ["Valor", fmtCurrency(c.valor_global)],
              ["Assinatura", fmtDate(c.data_assinatura)],
              ["Vigência", c.data_vigencia_inicio && c.data_vigencia_fim ? `${fmtDate(c.data_vigencia_inicio)} a ${fmtDate(c.data_vigencia_fim)}` : "-"],
              ["Orgão", payload.orgao?.nome || "-"],
              ["Fornecedor", payload.fornecedor?.nome || "-"],
              ["Tipo Fornecedor", payload.fornecedor?.tipo_pessoa || "-"],
              ["Situação", c.situacao_contrato || "-"],
            ];
            if (payload.empenhos && payload.empenhos.length > 0) {
              rows.push(["Empenhos", payload.empenhos.length]);
              const totalEmpenhado = payload.empenhos.reduce((sum, e) => sum + (Number(e.valor) || 0), 0);
              rows.push(["Total Empenhado", fmtCurrency(totalEmpenhado)]);
            }
          } else if (nodeLabel === "Sancao") {
            const s = payload.sancao || {};
            titleText = s.tipo_sancao || titleText;
            rows = [
              ["Tipo", s.tipo_sancao || "-"],
              ["Início", fmtDate(s.data_inicio_sancao)],
              ["Fim", fmtDate(s.data_fim_sancao)],
              ["Órgão", s.orgao_sancionador || "-"],
              ["Motivo", s.motivo_sancao || "-"],
              ["Publicação", fmtDate(s.data_publicacao)],
              ["Empresa", payload.empresa?.razao_social || "-"],
              ["CNPJ", maskCNPJ(payload.empresa?.cnpj)],
            ];
          }

          body.innerHTML = `
            <div class="popup-label" style="color:${color}">${nodeLabel}</div>
            <div class="popup-title">${titleText}</div>
            <div class="popup-rows">
              ${rows.filter(([, v]) => v !== "-" && v !== null && v !== undefined)
                .map(([k, v]) => `<div class="popup-row"><span class="popup-row-key">${k}</span><span class="popup-row-val">${v}</span></div>`)
                .join("")}
            </div>
            <div class="popup-actions">${actions}</div>
          `;
        }).catch(() => {
          body.innerHTML += `<div style="color:#ff6b35;font-size:12px;margin-top:8px;">Não foi possível carregar dados detalhados.</div>`;
          body.querySelector(".popup-loading")?.remove();
          body.innerHTML += `<div class="popup-actions"><a class="popup-btn popup-btn-primary" href="${profileUrl}">Ver perfil</a></div>`;
        });
      }

      // ── Fim popup ─────────────────────────────────────────────────────────

      const graph = await apiFetch(`/graph/expand?label=${encodeURIComponent(tipo)}&id=${encodeURIComponent(id)}&hops=1&max_nodes=100`);
      await renderGraph2D(cytoscape, graph, async (nextLabel, nextId, data, cyEvent) => {
        showNodePopup(nextLabel, nextId, data, cyEvent);
        $("#graph-meta").textContent = `${data.label} selecionado.`;
      });
      $("#graph-meta").textContent = `${graph.nodes.length} nós e ${graph.edges.length} arestas carregados. Clique em um nó para ver detalhes.`;
    } catch (error) {
      $("#profile-root").innerHTML = `<div class="error-state">${error.message}</div>`;
    }
  }

  loadProfile();
}

function renderPrimaryDetails(tipo, payload) {
  if (tipo === "Pessoa") {
    const pessoa = payload.pessoa || {};
    return [
      ["CPF", maskCPF(pessoa.cpf)],
      ["Nome", pessoa.nome || "-"],
      ["Nascimento", fmtDate(pessoa.dt_nascimento)],
      ["Parlamentar vinculado", payload.parlamentar?.nome_parlamentar || "-"],
    ].map(([label, value]) => `<div class="detail-item"><div class="stat-label">${label}</div><div class="detail-title">${value}</div></div>`).join("");
  }

  if (tipo === "Empresa") {
    const empresa = payload.empresa || {};
    return [
      ["CNPJ", maskCNPJ(empresa.cnpj_basico)],
      ["Razão social", empresa.razao_social || "-"],
      ["UF", empresa.uf || "-"],
      ["Situação", empresa.situacao_cadastral || "-"],
    ].map(([label, value]) => `<div class="detail-item"><div class="stat-label">${label}</div><div class="detail-title">${value}</div></div>`).join("");
  }

  const parlamentar = payload.parlamentar || {};
  return [
    ["Nome", parlamentar.nome_autor || "-"],
    ["CPF", maskCPF(parlamentar.cpf)],
    ["Partido", parlamentar.sigla_partido || parlamentar.partido || "-"],
    ["UF", parlamentar.uf || "-"],
  ].map(([label, value]) => `<div class="detail-item"><div class="stat-label">${label}</div><div class="detail-title">${value}</div></div>`).join("");
}

function renderSimpleTable(columns, rows) {
  if (!rows || !rows.length) {
    return `<div class="empty-state">Sem registros nessa seção.</div>`;
  } else {
    return `
      <div class="table-wrap">
        <table>
          <thead><tr>${columns.map((column) => `<th>${column.label}</th>`).join("")}</tr></thead>
          <tbody>
            ${rows.map((row) => `
              <tr>${columns.map((column) => `<td>${column.render ? column.render(row[column.key], row) : (row[column.key] ?? "-")}</td>`).join("")}</tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    `;
  }

}

function renderEmpresaSections(payload) {
  return `
    <div class="card-title">Relacionamentos empresariais</div>
    ${renderSimpleTable(
      [
        { key: "nome", label: "Socio PF" },
        { key: "cpf", label: "CPF", render: (value) => `<span class="mono">${maskCPF(value)}</span>` },
        { key: "qualificacao", label: "Qualificacao" },
      ],
      payload.socios_pf || []
    )}
    <div style="height:16px;"></div>
    ${renderSimpleTable(
      [
        { key: "id", label: "Contrato" },
        { key: "objeto", label: "Objeto" },
        { key: "valor", label: "Valor", render: (value) => `<span class="mono">${fmtCurrency(value)}</span>` },
        { key: "ano", label: "Ano" },
      ],
      payload.contratos || []
    )}
    <div style="height:16px;"></div>
    ${renderSimpleTable(
      [
        { key: "tipo", label: "Sancao" },
        { key: "inicio", label: "Inicio", render: (value) => fmtDate(value) },
        { key: "orgao", label: "Orgao" },
      ],
      payload.sancoes || []
    )}
  `;
}

function renderPessoaSections(payload) {
  return `
    <div class="card-title">Vínculos da pessoa</div>
    ${renderSimpleTable(
      [
        { key: "razao_social", label: "Empresa" },
        { key: "cnpj_basico", label: "CNPJ", render: (value) => `<span class="mono">${maskCNPJ(value)}</span>` },
        { key: "qualificacao", label: "Qualificacao" },
      ],
      payload.socios || []
    )}
    <div style="height:16px;"></div>
    ${renderSimpleTable(
      [
        { key: "ano", label: "Ano" },
        { key: "cargo", label: "Cargo" },
        { key: "nome_urna", label: "Nome de urna" },
        { key: "partido", label: "Partido" },
      ],
      payload.candidaturas || []
    )}
  `;
}

function renderParlamentarSections(payload) {
  return `
    <div class="card-title">Atividade parlamentar</div>
    ${renderSimpleTable(
      [
        { key: "codigo", label: "Emenda" },
        { key: "ano", label: "Ano" },
        { key: "municipio", label: "Municipio" },
        { key: "valor_pago", label: "Pago", render: (value) => `<span class="mono">${fmtCurrency(value)}</span>` },
      ],
      payload.emendas || []
    )}
    <div style="height:16px;"></div>
    ${renderSimpleTable(
      [
        { key: "razao_social", label: "Empresa" },
        { key: "cnpj_basico", label: "CNPJ", render: (value) => `<span class="mono">${maskCNPJ(value)}</span>` },
        { key: "qtd_emendas", label: "Emendas" },
        { key: "total_empenhado", label: "Total", render: (value) => `<span class="mono">${fmtCurrency(value)}</span>` },
      ],
      payload.empresas_beneficiadas || []
    )}
  `;
}

function mountCorruptionPage() {
  const params = new URLSearchParams(window.location.search);
  const cnpjFromQuery = params.get("cnpj");
  const root = $("#page-content");

  // ── Renderizador de padrões (compartilhado pelos dois modos) ─────────────
  async function loadCompanyPatterns(cnpj, containerSelector) {
    const container = $(containerSelector);
    container.innerHTML = `<div class="loading-state">Carregando padrões da empresa...</div>`;
    try {
      const payload = await apiFetch(`/patterns/empresa/${encodeURIComponent(cnpj)}`);
      const active = (payload.patterns || []).filter((pattern) => pattern.triggered);
      if (!active.length) {
        container.innerHTML = `<div class="empty-state">Nenhum padrão ativo para ${maskCNPJ(cnpj)}.</div>`;
        return;
      }
      container.innerHTML = `
        <div class="link-row" style="margin-bottom:12px;">
          <a class="button secondary" href="${buildProfileUrl("Empresa", cnpj)}">Abrir perfil da empresa</a>
        </div>
        <div class="detail-list">
          ${active.map((pattern) => `
            <div class="risk-item ${pattern.risk_level}">
              <div style="display:flex; justify-content:space-between; gap:12px; align-items:flex-start;">
                <div>
                  <div class="detail-title">${pattern.name_pt}</div>
                  <div class="muted" style="margin-top:4px;">${pattern.count} ocorrencia(s)</div>
                </div>
                <span class="badge" style="color:${pattern.risk_level === "high" ? "#ff3860" : pattern.risk_level === "medium" ? "#ff6b35" : "#ffd60a"}">${pattern.risk_level}</span>
              </div>
              <div class="muted mono" style="margin-top:10px;">valor total ${fmtCurrency(pattern.valor_total)}</div>
              <div class="detail-list" style="margin-top:12px;">
                ${(pattern.evidence || []).map((item) => `
                  <div class="detail-item">
                    <div class="stat-label">${item.tipo}</div>
                    <div>${item.label}</div>
                  </div>
                `).join("")}
              </div>
            </div>
          `).join("")}
        </div>
      `;
    } catch (error) {
      container.innerHTML = `<div class="error-state">${error.message}</div>`;
    }
  }

  // ── MODO DIRETO: veio com ?cnpj= na URL ──────────────────────────────────
  if (cnpjFromQuery) {
    root.innerHTML = `
      ${renderHeader({
        label: "Padrões de corrupcao",
        title: "Padrões de risco da empresa",
        subtitle: `Análise direta para o CNPJ ${maskCNPJ(cnpjFromQuery)}. <a href="/corrupcao.html" style="color:var(--blue, #38bdf8);text-decoration:none;">← Voltar para busca por estado</a>`,
      })}
      <section class="card">
        <div class="link-row" style="margin-bottom: 0;">
          <div>
            <div class="stat-label">CNPJ</div>
            <div class="result-title mono">${maskCNPJ(cnpjFromQuery)}</div>
          </div>
          <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
            <a class="button secondary" href="${buildProfileUrl("Empresa", cnpjFromQuery)}">Ver perfil completo</a>
            <a class="button subtle" href="/corrupcao.html">Busca por estado</a>
          </div>
        </div>
      </section>
      <section class="card">
        <div class="card-title">Padrões disparados</div>
        <div id="pattern-results" class="muted">Carregando...</div>
      </section>
    `;

    loadCompanyPatterns(cnpjFromQuery, "#pattern-results");
    return;
  }

  // ── MODO ESTADO: fluxo original ───────────────────────────────────────────
  root.innerHTML = `
    ${renderHeader({
      label: "Padrões de corrupcao",
      title: "Busca estadual de sinais de risco",
      subtitle: "Padrões de corrupção pré-definidos. Selecione uma para ver os detalhes.",
    })}
    <section class="card">
      <div class="toolbar">
        <div class="field" style="grid-column: span 3;">
          <label>UF</label>
          <select id="uf-select">
            ${["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"].map((uf) => `<option value="${uf}">${uf}</option>`).join("")}
          </select>
        </div>
        <div class="field" style="grid-column: span 3;">
          <label>Quantidade</label>
          <select id="uf-qty">
            <option value="10">10</option>
            <option value="20">20</option>
            <option value="30">30</option>
          </select>
        </div>
        <div class="field" style="grid-column: span 3;">
          <label>&nbsp;</label>
          <button class="button" id="uf-search">Buscar estado</button>
        </div>
      </div>
    </section>
    <section class="split">
      <div class="card">
        <div class="card-title">Empresas priorizadas</div>
        <div id="state-results" class="results-list"></div>
      </div>
      <div class="card">
        <div class="card-title">Padrões disparados</div>
        <div id="pattern-results" class="muted">Selecione uma empresa para abrir os padrões.</div>
      </div>
    </section>
  `;

  async function loadState() {
    const uf = $("#uf-select").value;
    const qty = $("#uf-qty").value;
    $("#state-results").innerHTML = `<div class="loading-state">Consultando empresas do estado...</div>`;
    $("#pattern-results").innerHTML = `<div class="muted">Selecione uma empresa para abrir os padrões.</div>`;

    try {
      const payload = await apiFetch(`/patterns/estado/${encodeURIComponent(uf)}?quantidade=${qty}`);
      if (!payload.empresas.length) {
        $("#state-results").innerHTML = `<div class="empty-state">Nenhuma empresa com padrão disparado em ${uf}.</div>`;
        return;
      }

      $("#state-results").innerHTML = payload.empresas.map((empresa) => `
        <div class="result-item">
          <div>${labelBadge("Empresa")}</div>
          <div>
            <div class="result-title">${empresa.empresa || empresa.cnpj_basico}</div>
            <div class="muted mono" style="margin-top:4px;">${maskCNPJ(empresa.cnpj_basico)}</div>
          </div>
          <div>
            <button class="button subtle company-patterns" data-cnpj="${empresa.cnpj_basico}">${empresa.triggered_count} padrões</button>
          </div>
        </div>
      `).join("");

      document.querySelectorAll(".company-patterns").forEach((button) => {
        button.addEventListener("click", () => loadCompanyPatterns(button.dataset.cnpj, "#pattern-results"));
      });
    } catch (error) {
      $("#state-results").innerHTML = `<div class="error-state">${error.message}</div>`;
    }
  }

  $("#uf-search").addEventListener("click", loadState);
  loadState();
}

function mountPipelinesPage() {
  const root = $("#page-content");
  root.innerHTML = `
    ${renderHeader({
      label: "Status dos pipeline",
      title: "Monitoramento dos ingestion runs",
      subtitle: "Essa tela usa uma rota nova da API para acompanhar o ultimo estado de cada pipeline sem depender do CLI.",
    })}
    <div id="pipelines-root" class="stack">
      <div class="loading-state">Carregando status...</div>
    </div>
  `;

  async function loadPipelines() {
    try {
      const payload = await apiFetch("/pipelines/status");
      $("#pipelines-root").innerHTML = `
        <section class="grid-3">
          <div class="card stat">
            <div class="stat-label">Pipelines OK</div>
            <div class="stat-value" style="color:var(--green);">${fmtNumber(payload.summary.ok)}</div>
          </div>
          <div class="card stat">
            <div class="stat-label">Em execucao</div>
            <div class="stat-value" style="color:var(--yellow);">${fmtNumber(payload.summary.running)}</div>
          </div>
          <div class="card stat">
            <div class="stat-label">Com erro</div>
            <div class="stat-value" style="color:var(--red);">${fmtNumber(payload.summary.error)}</div>
          </div>
        </section>
        <section class="card">
          <div class="card-title">Ultimos runs por fonte</div>
          ${renderSimpleTable(
            [
              { key: "source", label: "Pipeline" },
              { key: "status", label: "Status", render: (value, row) => {
                const color = row.status_group === "ok" ? "#00ff94" : row.status_group === "running" ? "#ffd60a" : "#ff3860";
                return `<span class="badge" style="color:${color}; background:${color}18">${value || "-"}</span>`;
              }},
              { key: "rows_in", label: "Linhas IN", render: (value) => `<span class="mono">${fmtNumber(value)}</span>` },
              { key: "rows_out", label: "Linhas OUT", render: (value) => `<span class="mono">${fmtNumber(value)}</span>` },
              { key: "started_at", label: "Inicio", render: (value) => `<span class="mono">${fmtDate(value)}</span>` },
              { key: "error", label: "Erro" },
            ],
            payload.items || []
          )}
        </section>
      `;
    } catch (error) {
      $("#pipelines-root").innerHTML = `<div class="error-state">${error.message}</div>`;
    }
  }

  loadPipelines();
}

window.DabertoFrontend = {
  buildShell,
  mountSearchPage,
  mountGraphPage,
  mountProfilePage,
  mountCorruptionPage,
  mountPipelinesPage,
};