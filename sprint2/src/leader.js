// leader.js
// Sprint 1 - Criação da API para submissão de um ficheiro
// Sprint 1 - Implementação do routing entre peers
// Sprint 2 - Atualização do vetor de documentos
//
// Critérios de aceitação (Sprint 2):
// - O cliente submete um ficheiro através da API do líder
// - Qualquer um dos peers recebe o novo vetor de CIDs propagado pelo líder
//   e os embeddings do ficheiro
//
// Nota: nesta fase o líder é estático (sem eleição entre peers), e não há
// ainda confirmação/commit por parte dos peers (isso é Sprint 3 / Sprint 5).
// O "vetor pendente" cresce localmente a cada documento submetido, sem
// substituir a versão confirmada (que, nesta fase, permanece vazia).

import express from 'express';
import multer from 'multer';
import { create } from 'kubo-rpc-client';
import { pipeline } from '@huggingface/transformers';
import { createNode } from './libp2p-node.js';
import { TOPIC_BROADCAST, TOPIC_PROPOSALS, LEADER_PORT } from './protocol.js';

const HTTP_PORT = process.env.LEADER_HTTP_PORT || 8000;
const IPFS_API_URL = process.env.IPFS_API_URL || 'http://127.0.0.1:5001';
const BROADCAST_INTERVAL_MS = 5000;

const ipfs = create({ url: IPFS_API_URL });
const upload = multer({ storage: multer.memoryStorage() });

// ===== Estado do vetor de documentos =====
// confirmedCids: versão confirmada pelos peers (imutável nesta fase - Sprint 5 trata disto)
// pendingCids: nova versão do vetor, ainda não confirmada, cresce a cada documento submetido
let confirmedCids = [];
let pendingCids = [];
let pendingVersion = 0;

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
