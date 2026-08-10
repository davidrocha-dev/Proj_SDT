// peer.js
// Sprint 1 - Implementação do routing entre peers
// Sprint 2 - Atualização do vetor de documentos (lado do peer)
// Sprint 3 - Atualização do vetor de documentos e confirmação
//
// Critérios de aceitação (Sprint 3):
// - O cliente submete um ficheiro através da API do líder
// - Cada um dos peers, após receber o pedido de atualização do líder, armazena a nova
//   versão do vetor (ou diff do vetor) e os embeddings em estruturas temporárias para
//   futura atualização após confirmação do líder
// - Após o processamento pelos peers, os hashes são enviados para o líder
//
// Nota: a resolução de conflitos de versão fica "a implementar no futuro" (conforme o
// card) - aqui apenas detetamos o conflito, registamos e não avançamos com os passos
// seguintes. O commit definitivo (líder agregar maioria + substituir versão) e a
// indexação FAISS/hnswlib ficam para o Sprint 5.

import { createHash } from 'node:crypto';
import { multiaddr } from '@multiformats/multiaddr';
import { createNode } from './libp2p-node.js';
import { TOPIC_BROADCAST, TOPIC_PROPOSALS, TOPIC_CONFIRMS, LEADER_MULTIADDR } from './protocol.js';

const PEER_ID_LABEL = process.env.PEER_LABEL || `peer-${process.pid}`;

// Estado local do peer:
// - confirmedVersion / confirmedCids: última versão que este peer já confirmou (commit real
//   só acontece no Sprint 5; nesta fase fica em 0/[] e serve de referência para deteção de conflito)
// - pending: version -> { cid, cids, embeddings } (armazenamento temporário até ao commit)
let confirmedVersion = 0;
let confirmedCids = [];
const pending = new Map();

function hashCids(cids) {
  return createHash('sha256').update(cids.join(',')).digest('hex');
}

function handleBroadcast(data) {
  const msg = JSON.parse(new TextDecoder().decode(data));
  console.log(`[${PEER_ID_LABEL}] Mensagem recebida do líder: "${msg.text}" (seq=${msg.seq}, ts=${msg.ts})`);
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
      }
    } catch (err) {
      console.error(`[${PEER_ID_LABEL}] Erro ao processar mensagem:`, err.message);
    }
  });

  node.services.pubsub.subscribe(TOPIC_BROADCAST);
  node.services.pubsub.subscribe(TOPIC_PROPOSALS);

  console.log(`[${PEER_ID_LABEL}] A ligar ao líder em ${LEADER_MULTIADDR} ...`);
  await dialWithRetry(node, LEADER_MULTIADDR);
  console.log(`[${PEER_ID_LABEL}] Ligado ao líder. À espera de mensagens...`);
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
