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
            <div class="logo-sub">Inteligencia Civica</div>
          </a>
        </div>
        <nav class="nav">
          <section class="nav-section">
            <div class="nav-label">Navegacao</div>
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
      subtitle: "Use a base visual do design que voce gostou, mas com uma tela pratica para localizar pessoas, empresas, parlamentares e outros nos do grafo.",
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
      <div id="search-feedback" class="muted">Digite pelo menos 2 caracteres para consultar o indice fulltext.</div>
      <div id="search-results" class="results-list"></div>
    </section>
  `;

  async function runSearch() {
    const q = $("#search-query").value.trim();
    const label = $("#search-label").value;
    const feedback = $("#search-feedback");
    const container = $("#search-results");
    container.innerHTML = "";

    if (q.length < 2) {
      feedback.textContent = "Digite pelo menos 2 caracteres.";
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
          <div class="muted mono">score ${Number(item.score || 0).toFixed(2)}</div>
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
    ...graph.nodes.map((node) => ({
      data: {
        id: node.uid,
        label: node.nome || node.razao_social || node.uid,
        nodeLabel: node.label,
        color: LABEL_COLORS[node.label] || LABEL_COLORS.default,
      },
    })),
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
        },
      },
    ],
  });

  cy.on("tap", "node", (event) => {
    const id = event.target.id();
    const [label, rawId] = id.split(":");
    if (label && rawId) onNodeTap(label, rawId, event.target.data());
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
      subtitle: "Entre com a entidade raiz e navegue pelo grafo sem passar por uma landing page. A selecao aqui fica focada em operacao e leitura.",
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
        <div class="field" style="grid-column: span 5;">
          <label>Identificador</label>
          <input id="graph-id" placeholder="CPF, CNPJ basico, id parlamentar, UF..." />
        </div>
        <div class="field" style="grid-column: span 2;">
          <label>Visualizacao</label>
          <div class="mode-toggle">
            <button type="button" class="mode-option active" data-mode="2d">2D</button>
            <button type="button" class="mode-option" data-mode="3d">3D</button>
          </div>
        </div>
        <div class="field" style="grid-column: span 2;">
          <label>&nbsp;</label>
          <button class="button" id="graph-load">Expandir</button>
        </div>
      </div>
    </section>
    <section id="graph-layout" class="split graph-layout">
      <div class="card">${makeGraphShell()}</div>
      <div class="stack">
        <section class="card">
          <div class="card-title">Selecao atual</div>
          <div id="graph-selection" class="muted">Nenhum no selecionado.</div>
        </section>
        <section class="card">
          <div class="card-title">Estatisticas</div>
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
      nodes: (graph.nodes || []).map((node) => ({
        id: node.uid,
        uid: node.uid,
        label: node.label,
        name: node.nome || node.razao_social || node.uid,
        color: LABEL_COLORS[node.label] || LABEL_COLORS.default,
        val: 5,
      })),
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
      $("#graph-meta").textContent = `Expandindo conexoes de ${data.label}...`;

      try {
        const expanded = await apiFetch(`/graph/expand?label=${encodeURIComponent(nextLabel)}&id=${encodeURIComponent(nextId)}&hops=1&max_nodes=80`);
        currentGraph = mergeGraphData(currentGraph, expanded);
        $("#graph-stats").innerHTML = `
          <span class="pill">${currentGraph.nodes.length} nos</span>
          <span class="pill">${currentGraph.edges.length} arestas</span>
          <span class="pill">ultimo grau ${expanded.meta.degree}</span>
          <span class="pill">expansao acumulada</span>
        `;
        await redrawGraph();
        $("#graph-meta").textContent = `Conexoes de ${data.label} adicionadas ao grafo.`;
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
        <span class="pill">${currentGraph.nodes.length} nos</span>
        <span class="pill">${currentGraph.edges.length} arestas</span>
        <span class="pill">grau raiz ${currentGraph.meta.degree}</span>
        <span class="pill">${currentGraph.meta.is_supernode ? "limite superno ativo" : "expansao completa"}</span>
      `;

      await redrawGraph();
      $("#graph-meta").textContent = "Clique em um no para adicionar novas conexoes ao grafo atual.";
    } catch (error) {
      $("#graph-meta").textContent = "Nao foi possivel carregar o grafo.";
      $("#graph-selection").innerHTML = `<div class="error-state">${error.message}</div>`;
    }
  }

  $("#graph-load").addEventListener("click", () => {
    loadGraph($("#graph-label").value, $("#graph-id").value.trim(), 1);
  });

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
      subtitle: "Aqui juntamos os dados do perfil e o grafo expandivel num fluxo mais limpo que o prototipo original.",
    })}
    <div id="profile-root" class="stack">
      <div class="loading-state">Carregando perfil...</div>
    </div>
  `;

  if (!tipo || !id) {
    $("#profile-root").innerHTML = `<div class="error-state">Parametros tipo e id sao obrigatorios.</div>`;
    return;
  }

  const endpointMap = {
    Pessoa: `/pessoa/${encodeURIComponent(id)}`,
    Empresa: `/empresa/${encodeURIComponent(id)}`,
    Parlamentar: `/parlamentar/${encodeURIComponent(id)}`,
  };

  const endpoint = endpointMap[tipo];
  if (!endpoint) {
    $("#profile-root").innerHTML = `<div class="error-state">Tipo ${tipo} ainda nao tem tela estruturada.</div>`;
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
            ${tipo === "Empresa" ? `<a class="button subtle" href="/corrupcao.html?cnpj=${encodeURIComponent(id)}">Padroes por estado</a>` : ""}
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

      const graph = await apiFetch(`/graph/expand?label=${encodeURIComponent(tipo)}&id=${encodeURIComponent(id)}&hops=1&max_nodes=100`);
      await renderGraph2D(cytoscape, graph, async (nextLabel, nextId, data) => {
        $("#graph-meta").textContent = `${data.label} selecionado. Abra o perfil dedicado para aprofundar.`;
      });
      $("#graph-meta").textContent = `${graph.nodes.length} nos e ${graph.edges.length} arestas carregados.`;
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
      ["Razao social", empresa.razao_social || "-"],
      ["UF", empresa.uf || "-"],
      ["Situacao", empresa.situacao_cadastral || "-"],
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
    return `<div class="empty-state">Sem registros nessa secao.</div>`;
  }

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
    <div class="card-title">Vinculos da pessoa</div>
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
  root.innerHTML = `
    ${renderHeader({
      label: "Padroes de corrupcao",
      title: "Busca estadual de sinais de risco",
      subtitle: "Aqui a navegacao vira uma triagem por UF. A API devolve as empresas com mais padroes disparados e a tela permite abrir os detalhes dos padroes por empresa.",
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
        <div class="card-title">Padroes disparados</div>
        <div id="pattern-results" class="muted">Selecione uma empresa para abrir os padroes.</div>
      </div>
    </section>
  `;

  async function loadState() {
    const uf = $("#uf-select").value;
    const qty = $("#uf-qty").value;
    $("#state-results").innerHTML = `<div class="loading-state">Consultando empresas do estado...</div>`;
    $("#pattern-results").textContent = "Selecione uma empresa para abrir os padroes.";

    try {
      const payload = await apiFetch(`/patterns/estado/${encodeURIComponent(uf)}?quantidade=${qty}`);
      if (!payload.empresas.length) {
        $("#state-results").innerHTML = `<div class="empty-state">Nenhuma empresa com padrao disparado em ${uf}.</div>`;
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
            <button class="button subtle company-patterns" data-cnpj="${empresa.cnpj_basico}">${empresa.triggered_count} padroes</button>
          </div>
        </div>
      `).join("");

      document.querySelectorAll(".company-patterns").forEach((button) => {
        button.addEventListener("click", () => loadCompanyPatterns(button.dataset.cnpj));
      });

      if (cnpjFromQuery) {
        loadCompanyPatterns(cnpjFromQuery);
      }
    } catch (error) {
      $("#state-results").innerHTML = `<div class="error-state">${error.message}</div>`;
    }
  }

  async function loadCompanyPatterns(cnpj) {
    $("#pattern-results").innerHTML = `<div class="loading-state">Carregando padroes da empresa...</div>`;
    try {
      const payload = await apiFetch(`/patterns/empresa/${encodeURIComponent(cnpj)}`);
      const active = (payload.patterns || []).filter((pattern) => pattern.triggered);
      if (!active.length) {
        $("#pattern-results").innerHTML = `<div class="empty-state">Nenhum padrao ativo para ${maskCNPJ(cnpj)}.</div>`;
        return;
      }

      $("#pattern-results").innerHTML = `
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
      $("#pattern-results").innerHTML = `<div class="error-state">${error.message}</div>`;
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
