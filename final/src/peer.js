// peer.js
// Sprint 1 - Implementação do routing entre peers
// Sprint 2 - Atualização do vetor de documentos (lado do peer)
// Sprint 3 - Atualização do vetor de documentos e confirmação
// Sprint 5 - Atualização do vetor em todos os peers (commit + indexação hnswlib)
// Sprint 5 - RNF3 (Parte 1) - deteção da falha do líder
//
// Critérios de aceitação (Sprint 5):
// - O peer recebe o commit
// - Substitui a versão atual do vetor de CIDs pela nova versão
// - Atualiza a indexação hnswlib (armazenada em memória, equivalente a FAISS)
// - Após terminar o processo do líder, os peers devem detetar a falha após n segundos
//
// Nota (RNF3 Parte 1): apenas falhas fail-stop são consideradas (o líder para de
// responder e não recupera sozinho); não há ainda re-eleição nem recuperação automática
// de ficheiros pinned (isso fica para trabalho futuro, conforme o card).

import { createHash } from 'node:crypto';
import { multiaddr } from '@multiformats/multiaddr';
import hnswlibPkg from 'hnswlib-node';
import { createNode } from './libp2p-node.js';
import { TOPIC_BROADCAST, TOPIC_PROPOSALS, TOPIC_CONFIRMS, TOPIC_COMMITS, LEADER_MULTIADDR } from './protocol.js';

const { HierarchicalNSW } = hnswlibPkg;

const PEER_ID_LABEL = process.env.PEER_LABEL || `peer-${process.pid}`;
const HNSW_MAX_ELEMENTS = 10000;

// O líder publica um broadcast a cada 5s (BROADCAST_INTERVAL_MS em leader.js), que
// funciona como heartbeat. n segundos sem heartbeat = líder considerado em falha.
// Usamos ~2.4x o intervalo do líder para tolerar a perda pontual de 1 mensagem sem
// gerar falsos positivos.
const HEARTBEAT_TIMEOUT_MS = Number(process.env.HEARTBEAT_TIMEOUT_MS || 12000);
const HEARTBEAT_CHECK_INTERVAL_MS = 1000;

// Estado local do peer:
// - confirmedVersion / confirmedCids: última versão confirmada por commit do líder
// - pending: version -> { cid, cids, embeddings } (armazenamento temporário até ao commit)
// - hnsw / cidByLabel: índice vetorial em memória e mapeamento label (inteiro) -> CID
let confirmedVersion = 0;
let confirmedCids = [];
const pending = new Map();

let hnsw = null;
const cidByLabel = [];

// Estado da deteção de falha do líder (heartbeat)
let lastHeartbeatTs = null;
let leaderFailureDetected = false;

function hashCids(cids) {
  return createHash('sha256').update(cids.join(',')).digest('hex');
}

function ensureIndex(dim) {
  if (!hnsw) {
    hnsw = new HierarchicalNSW('cosine', dim);
    hnsw.initIndex(HNSW_MAX_ELEMENTS);
    console.log(`[${PEER_ID_LABEL}] Índice hnswlib criado (dim=${dim}, capacidade=${HNSW_MAX_ELEMENTS}).`);
  }
}

function handleBroadcast(data) {
  const msg = JSON.parse(new TextDecoder().decode(data));
  console.log(`[${PEER_ID_LABEL}] Mensagem recebida do líder: "${msg.text}" (seq=${msg.seq}, ts=${msg.ts})`);

  lastHeartbeatTs = Date.now();
  if (leaderFailureDetected) {
    leaderFailureDetected = false;
    console.log(`[${PEER_ID_LABEL}] Líder voltou a responder - falha anterior considerada recuperada.`);
  }
}

// RNF3 (Parte 1): deteta a falha do líder por ausência de heartbeat durante n segundos.
function monitorLeaderHeartbeat() {
  setInterval(() => {
    if (lastHeartbeatTs === null || leaderFailureDetected) return;

    const elapsed = Date.now() - lastHeartbeatTs;
    if (elapsed > HEARTBEAT_TIMEOUT_MS) {
      leaderFailureDetected = true;
      console.error(
        `[${PEER_ID_LABEL}] FALHA DO LÍDER DETETADA: sem heartbeat há ${Math.round(elapsed / 1000)}s ` +
          `(limite=${HEARTBEAT_TIMEOUT_MS / 1000}s).`
      );
    }
  }, HEARTBEAT_CHECK_INTERVAL_MS);
}

async function handleProposal(node, data) {
  const proposal = JSON.parse(new TextDecoder().decode(data));
  const { version, cid, cids, embeddings } = proposal;

  // 1. verifica conflito de versões
  const expectedVersion = confirmedVersion + 1;
  if (version !== expectedVersion) {
    console.warn(
      `[${PEER_ID_LABEL}] CONFLITO de versão na proposta recebida: esperava v${expectedVersion}, ` +
        `recebi v${version}. Resolução de conflitos a implementar no futuro - proposta ignorada.`
    );
    return; // não executa os próximos passos
  }

  // 2. cria nova versão do vetor de CIDs (sem substituir a versão confirmada)
  //    e 3. armazena temporariamente os embeddings para futura indexação FAISS
  pending.set(version, { cid, cids, embeddings });

  // 4. calcula e devolve a hash do vetor de CIDs ao líder
  const hash = hashCids(cids);
  const confirmMsg = {
    type: 'confirm',
    version,
    hash,
    peerId: node.peerId.toString(),
    label: PEER_ID_LABEL,
  };
  const payload = new TextEncoder().encode(JSON.stringify(confirmMsg));
  await node.services.pubsub.publish(TOPIC_CONFIRMS, payload);

  console.log(
    `[${PEER_ID_LABEL}] Proposta v${version} aceite (CID=${cid}, vetor com ${cids.length} CIDs, ` +
      `embeddings dim=${embeddings.length}). Hash=${hash.slice(0, 12)}... enviada ao líder. ` +
      `(pendentes=${pending.size})`
  );
}

function handleCommit(data) {
  const commit = JSON.parse(new TextDecoder().decode(data));
  const { version, cids } = commit;

  if (version <= confirmedVersion) {
    console.log(`[${PEER_ID_LABEL}] Commit v${version} ignorado (já confirmado anteriormente).`);
    return;
  }
  if (version !== confirmedVersion + 1) {
    console.warn(
      `[${PEER_ID_LABEL}] Commit fora de sequência: esperava v${confirmedVersion + 1}, recebi v${version}. Ignorado.`
    );
    return;
  }

  const pend = pending.get(version);
  if (!pend) {
    console.warn(`[${PEER_ID_LABEL}] Commit v${version} sem dados pendentes correspondentes - ignorado.`);
    return;
  }

  // 1. substitui a versão atual do vetor de CIDs pela nova versão
  confirmedVersion = version;
  confirmedCids = cids;
  pending.delete(version);

  // 2. atualiza a indexação hnswlib (equivalente a FAISS) em memória
  ensureIndex(pend.embeddings.length);
  const label = cidByLabel.length;
  hnsw.addPoint(pend.embeddings, label);
  cidByLabel.push(pend.cid);

  console.log(
    `[${PEER_ID_LABEL}] COMMIT v${version} aplicado. Vetor confirmado com ${confirmedCids.length} CIDs. ` +
      `Indexado no hnswlib (label=${label}, total indexado=${cidByLabel.length}).`
  );
}

async function main() {
  const node = await createNode(0);

  console.log(`[${PEER_ID_LABEL}] Nó libp2p arrancado. PeerId: ${node.peerId.toString()}`);

  node.services.pubsub.addEventListener('message', (evt) => {
    try {
      if (evt.detail.topic === TOPIC_BROADCAST) {
        handleBroadcast(evt.detail.data);
      } else if (evt.detail.topic === TOPIC_PROPOSALS) {
        handleProposal(node, evt.detail.data).catch((err) =>
          console.error(`[${PEER_ID_LABEL}] Erro ao processar proposta:`, err.message)
        );
      } else if (evt.detail.topic === TOPIC_COMMITS) {
        handleCommit(evt.detail.data);
      }
    } catch (err) {
      console.error(`[${PEER_ID_LABEL}] Erro ao processar mensagem:`, err.message);
    }
  });

  node.services.pubsub.subscribe(TOPIC_BROADCAST);
  node.services.pubsub.subscribe(TOPIC_PROPOSALS);
  node.services.pubsub.subscribe(TOPIC_COMMITS);

  console.log(`[${PEER_ID_LABEL}] A ligar ao líder em ${LEADER_MULTIADDR} ...`);
  await dialWithRetry(node, LEADER_MULTIADDR);
  console.log(`[${PEER_ID_LABEL}] Ligado ao líder. À espera de mensagens...`);

  lastHeartbeatTs = Date.now(); // considera a ligação inicial como o primeiro "heartbeat"
  monitorLeaderHeartbeat();
}

// Tenta ligar ao líder algumas vezes antes de desistir - útil quando o peer arranca
// antes de o líder estar totalmente pronto (ex: ainda a carregar o modelo de embeddings).
async function dialWithRetry(node, addr, retries = 5, delayMs = 2000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await node.dial(multiaddr(addr));
      return;
    } catch (err) {
      if (attempt === retries) throw err;
      console.warn(
        `[${PEER_ID_LABEL}] Falha ao ligar ao líder (tentativa ${attempt}/${retries}): ${err.message}. A tentar novamente em ${delayMs}ms...`
      );
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

main().catch((err) => {
  console.error(`[${PEER_ID_LABEL}] Erro:`, err);
  process.exit(1);
});
