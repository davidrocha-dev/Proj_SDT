// leader.js
// Sprint 1 - Criação da API para submissão de um ficheiro
// Sprint 1 - Implementação do routing entre peers
// Sprint 2 - Atualização do vetor de documentos
// Sprint 3 - Atualização do vetor de documentos e confirmação (receção dos hashes)
// Sprint 5 - Atualização do vetor em todos os peers (maioria + commit)
//
// Critérios de aceitação (Sprint 5, parte do líder):
// - Depois de receber a maioria das respostas dos peers com hash correta,
//   envia um commit para todos os peers
// - Substitui a versão atual do vetor de CIDs pela nova versão
//
// Desafio do card: "como obter o número aproximado de peers, tendo em conta que existe
// desacoplamento espacial?" - resposta adotada: combinamos duas fontes disponíveis no
// líder, sem registo centralizado de membros:
//   1. `getSubscribers(TOPIC_CONFIRMS)` do GossipSub - lista de subscritores conhecidos
//      via anúncios de subscrição propagados pelo protocolo (pode estar temporariamente
//      desatualizada logo após o arranque, dado o desacoplamento espacial do pub/sub);
//   2. `node.getPeers()` do libp2p - peers com ligação direta estabelecida ao líder
//      (nesta topologia, todos os peers dialam o líder explicitamente, por isso este
//      número tende a ser o mais fiável e imediato).
// Usamos o maior dos dois valores como aproximação, e documentamos que continua a ser
// apenas uma estimativa - na prática observámos o cenário exato que o desafio previa:
// logo a seguir ao arranque, getSubscribers() pode devolver 0 antes da informação de
// subscrição se propagar, o que validou a necessidade de um segundo sinal (getPeers()).
//
// Nota: nesta fase o líder é estático (sem eleição entre peers).

import { createHash } from 'node:crypto';
import express from 'express';
import multer from 'multer';
import { create } from 'kubo-rpc-client';
import { pipeline } from '@huggingface/transformers';
import { createNode } from './libp2p-node.js';
import { TOPIC_BROADCAST, TOPIC_PROPOSALS, TOPIC_CONFIRMS, TOPIC_COMMITS, LEADER_PORT } from './protocol.js';

const HTTP_PORT = process.env.LEADER_HTTP_PORT || 8000;
const IPFS_API_URL = process.env.IPFS_API_URL || 'http://127.0.0.1:5001';
const BROADCAST_INTERVAL_MS = 5000;

const ipfs = create({ url: IPFS_API_URL });
const upload = multer({ storage: multer.memoryStorage() });

// ===== Estado do vetor de documentos =====
// confirmedCids/confirmedVersion: última versão com commit efetuado (substituída ao atingir maioria)
// pendingCids: nova versão do vetor, ainda não confirmada, cresce a cada documento submetido
let confirmedCids = [];
let confirmedVersion = 0;
let pendingCids = [];
let pendingVersion = 0;

// confirmations: version -> Map<peerId, { hash, valid }> — respostas recebidas dos peers por versão
const confirmations = new Map();

function hashCids(cids) {
  return createHash('sha256').update(cids.join(',')).digest('hex');
}

// Aproxima o número de peers ativos combinando subscritores do GossipSub e ligações diretas.
function estimatePeerCount(node) {
  let gossipSubscribers = 0;
  let directConnections = 0;
  try {
    gossipSubscribers = node.services.pubsub.getSubscribers(TOPIC_CONFIRMS).length;
  } catch {
    /* ignora - fica 0 */
  }
  try {
    directConnections = node.getPeers().length;
  } catch {
    /* ignora - fica 0 */
  }
  return Math.max(gossipSubscribers, directConnections);
}

async function commitVersion(node, version) {
  // Reconstrói o vetor de CIDs tal como estava nessa versão (pendingCids é cumulativo e
  // monótono, por isso um "slice" histórico é sempre válido mesmo que já existam versões
  // mais recentes pendentes).
  const cidsAtVersion = pendingCids.slice(0, version);

  confirmedVersion = version;
  confirmedCids = cidsAtVersion;

  const commitMsg = { type: 'commit', version, cids: cidsAtVersion };
  const payload = new TextEncoder().encode(JSON.stringify(commitMsg));
  await node.services.pubsub.publish(TOPIC_COMMITS, payload);

  confirmations.delete(version);
  console.log(
    `[Leader] COMMIT v${version} enviado a todos os peers (maioria atingida). ` +
      `Vetor confirmado com ${confirmedCids.length} CIDs.`
  );
}

// ===== Embeddings (lazy-load do modelo, reutilizado entre pedidos) =====
let extractorPromise = null;
function getExtractor() {
  if (!extractorPromise) {
    console.log('[Leader] A carregar o modelo de embeddings (all-MiniLM-L6-v2)...');
    extractorPromise = pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
  }
  return extractorPromise;
}

async function generateEmbeddings(text) {
  const extractor = await getExtractor();
  const output = await extractor(text, { pooling: 'mean', normalize: true });
  return Array.from(output.data);
}

// ===== Nó libp2p (arrancado em main(), usado pela rota /upload) =====
let p2pNode = null;

const app = express();

app.post('/upload', upload.single('file'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "Campo 'file' em falta no pedido." });
  }
  if (!p2pNode) {
    return res.status(503).json({ error: 'Nó P2P do líder ainda não está pronto.' });
  }

  try {
    // 1. guarda o documento no IPFS local
    const { cid } = await ipfs.add({
      path: req.file.originalname,
      content: req.file.buffer,
    });
    const cidStr = cid.toString();
    console.log(`[Leader] Ficheiro "${req.file.originalname}" adicionado ao IPFS -> CID: ${cidStr}`);

    // 2. gera embeddings do documento (assume texto simples)
    const text = req.file.buffer.toString('utf-8');
    const embeddings = await generateEmbeddings(text);
    console.log(`[Leader] Embeddings gerados (dimensão ${embeddings.length}).`);

    // 3. cria nova versão do vetor de CIDs (sem substituir confirmedCids)
    pendingVersion += 1;
    pendingCids = [...pendingCids, cidStr];

    // 4. propaga [versão, CID, embeddings] para os peers via GossipSub
    const proposal = {
      type: 'proposal',
      version: pendingVersion,
      cid: cidStr,
      cids: pendingCids,
      embeddings,
    };
    const payload = new TextEncoder().encode(JSON.stringify(proposal));
    await p2pNode.services.pubsub.publish(TOPIC_PROPOSALS, payload);
    console.log(`[Leader] Proposta v${pendingVersion} propagada (${pendingCids.length} CIDs no vetor).`);

    return res.status(200).json({
      status: 'OK',
      filename: req.file.originalname,
      size: req.file.size,
      cid: cidStr,
      version: pendingVersion,
    });
  } catch (err) {
    console.error('[Leader] Erro ao processar ficheiro:', err);
    return res.status(500).json({ error: 'Falha ao processar ficheiro.' });
  }
});

// ===== Routing entre peers (libp2p + GossipSub) =====

async function startP2P() {
  const node = await createNode(LEADER_PORT);

  console.log(`[Leader] Nó libp2p arrancado. PeerId: ${node.peerId.toString()}`);
  console.log('[Leader] Endereços a escutar:');
  node.getMultiaddrs().forEach((addr) => console.log('  ' + addr.toString()));

  node.services.pubsub.subscribe(TOPIC_BROADCAST);
  node.services.pubsub.subscribe(TOPIC_PROPOSALS);
  node.services.pubsub.subscribe(TOPIC_CONFIRMS);

  node.services.pubsub.addEventListener('message', (evt) => {
    if (evt.detail.topic !== TOPIC_CONFIRMS) return;
    try {
      const confirm = JSON.parse(new TextDecoder().decode(evt.detail.data));
      const { version, hash, peerId, label } = confirm;

      if (version <= confirmedVersion) {
        console.log(`[Leader] Confirm de ${label || peerId} para v${version} ignorado (já committada).`);
        return;
      }

      const cidsAtVersion = pendingCids.slice(0, version);
      const isValid = pendingCids.length >= version && hash === hashCids(cidsAtVersion);

      if (!confirmations.has(version)) confirmations.set(version, new Map());
      confirmations.get(version).set(peerId, { hash, valid: isValid });

      const entries = [...confirmations.get(version).values()];
      const validCount = entries.filter((e) => e.valid).length;

      console.log(
        `[Leader] Confirm recebido de ${label || peerId} para v${version}: ${hash.slice(0, 12)}... ` +
          `[${isValid ? 'hash OK' : 'HASH DIFERENTE/INVÁLIDA'}] (válidos=${validCount}/${entries.length})`
      );

      const peerCount = estimatePeerCount(node);
      const majority = Math.floor(peerCount / 2) + 1;

      if (validCount >= majority && confirmedVersion < version) {
        console.log(`[Leader] Maioria atingida para v${version} (${validCount}/${peerCount} peers estimados). A fazer commit...`);
        commitVersion(node, version).catch((err) => console.error('[Leader] Erro ao fazer commit:', err.message));
      }
    } catch (err) {
      console.error('[Leader] Erro ao processar confirm:', err.message);
    }
  });

  // Broadcast periódico (Sprint 1): serve também de heartbeat para o RNF3 Parte 1 -
  // os peers usam a ausência destas mensagens durante n segundos para detetar a falha
  // do líder (ver HEARTBEAT_TIMEOUT_MS em peer.js).
  let seq = 0;
  setInterval(() => {
    seq += 1;
    const message = {
      from: 'leader',
      seq,
      ts: new Date().toISOString(),
      text: `Mensagem de broadcast #${seq} enviada pelo líder`,
    };
    const payload = new TextEncoder().encode(JSON.stringify(message));

    node.services.pubsub
      .publish(TOPIC_BROADCAST, payload)
      .then(() => console.log(`[Leader] Broadcast enviado: ${message.text}`))
      .catch((err) => console.error('[Leader] Erro ao publicar broadcast:', err.message));
  }, BROADCAST_INTERVAL_MS);

  return node;
}

async function main() {
  p2pNode = await startP2P();

  app.listen(HTTP_PORT, () => {
    console.log(`[Leader] API REST pronta: http://localhost:${HTTP_PORT}/upload`);
    console.log(`[Leader] A usar IPFS (Kubo) em: ${IPFS_API_URL}`);
  });
}

main().catch((err) => {
  console.error('[Leader] Erro ao arrancar:', err);
  process.exit(1);
});
