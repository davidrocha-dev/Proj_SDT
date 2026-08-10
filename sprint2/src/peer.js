// peer.js
// Sprint 1 - Implementação do routing entre peers
// Sprint 2 - Atualização do vetor de documentos (lado do peer)
//
// Critérios de aceitação:
// - Envio de uma mensagem pelo líder que pode ser visualizada na consola de cada um dos peers (Sprint 1)
// - Qualquer um dos peers recebe o novo vetor de CIDs propagado pelo líder e os embeddings do ficheiro (Sprint 2)
//
// Nota: nesta fase o peer apenas armazena a proposta temporariamente (Map `pending`).
// A verificação de conflitos, o hash e a confirmação ao líder são Sprint 3.
// A aplicação definitiva (commit) e a indexação FAISS/hnswlib são Sprint 5.

import { multiaddr } from '@multiformats/multiaddr';
import { createNode } from './libp2p-node.js';
import { TOPIC_BROADCAST, TOPIC_PROPOSALS, LEADER_MULTIADDR } from './protocol.js';

const PEER_ID_LABEL = process.env.PEER_LABEL || `peer-${process.pid}`;

// version -> { cid, cids, embeddings }
const pending = new Map();

function handleBroadcast(data) {
  const msg = JSON.parse(new TextDecoder().decode(data));
  console.log(`[${PEER_ID_LABEL}] Mensagem recebida do líder: "${msg.text}" (seq=${msg.seq}, ts=${msg.ts})`);
}

function handleProposal(data) {
  const proposal = JSON.parse(new TextDecoder().decode(data));
  pending.set(proposal.version, {
    cid: proposal.cid,
    cids: proposal.cids,
    embeddings: proposal.embeddings,
  });
  console.log(
    `[${PEER_ID_LABEL}] Proposta recebida: v${proposal.version}, CID=${proposal.cid}, ` +
      `vetor com ${proposal.cids.length} CIDs, embeddings dim=${proposal.embeddings.length}. ` +
      `(armazenada temporariamente, total pendentes=${pending.size})`
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
        handleProposal(evt.detail.data);
      }
    } catch (err) {
      console.error(`[${PEER_ID_LABEL}] Erro ao processar mensagem:`, err.message);
    }
  });

  node.services.pubsub.subscribe(TOPIC_BROADCAST);
  node.services.pubsub.subscribe(TOPIC_PROPOSALS);

  console.log(`[${PEER_ID_LABEL}] A ligar ao líder em ${LEADER_MULTIADDR} ...`);
  await node.dial(multiaddr(LEADER_MULTIADDR));
  console.log(`[${PEER_ID_LABEL}] Ligado ao líder. À espera de mensagens...`);
}

main().catch((err) => {
  console.error(`[${PEER_ID_LABEL}] Erro:`, err);
  process.exit(1);
});
