// Teste rápido: confirma que conseguimos criar e arrancar um nó libp2p
// (TCP + noise + yamux + identify + gossipsub + kad-dht) antes de o
// integrar no leader.js/peer.js.

import { createLibp2p } from 'libp2p';
import { tcp } from '@libp2p/tcp';
import { identify } from '@libp2p/identify';
import { ping } from '@libp2p/ping';
import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import { kadDHT } from '@libp2p/kad-dht';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';

async function main() {
  const node = await createLibp2p({
    addresses: { listen: ['/ip4/0.0.0.0/tcp/0'] },
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      identify: identify(),
      ping: ping(),
      pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
      dht: kadDHT({ clientMode: false }),
    },
  });

  await node.start();

  console.log('[test-libp2p] Nó arrancado com sucesso.');
  console.log('[test-libp2p] PeerId:', node.peerId.toString());
  console.log('[test-libp2p] Endereços a escutar:');
  node.getMultiaddrs().forEach((addr) => console.log('  ' + addr.toString()));

  await node.stop();
  console.log('[test-libp2p] Nó parado. OK.');
}

main().catch((err) => {
  console.error('[test-libp2p] Erro:', err);
  process.exit(1);
});
