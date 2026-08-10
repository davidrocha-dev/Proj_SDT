// libp2p-node.js
// Factory partilhada para criar um nó libp2p (usado pelo líder e pelos peers).
// Inclui: TCP transport, noise (encriptação), yamux (multiplexagem),
// identify, ping (dependência do kad-dht), gossipsub (PubSub) e Kademlia DHT.

import { createLibp2p } from 'libp2p';
import { tcp } from '@libp2p/tcp';
import { identify } from '@libp2p/identify';
import { ping } from '@libp2p/ping';
import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import { kadDHT } from '@libp2p/kad-dht';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';

/**
 * Cria e arranca um nó libp2p.
 * @param {number} port - porta TCP a escutar (0 = porta aleatória)
 * @returns {Promise<import('libp2p').Libp2p>}
 */
export async function createNode(port = 0) {
  const node = await createLibp2p({
    addresses: { listen: [`/ip4/0.0.0.0/tcp/${port}`] },
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
  return node;
}
