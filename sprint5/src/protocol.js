// protocol.js
// Constantes partilhadas do protocolo de aplicação (nomes de tópicos GossipSub, etc.)

export const TOPIC_BROADCAST = 'sdt/broadcast/1.0.0';
export const TOPIC_PROPOSALS = 'sdt/proposals/1.0.0';
export const TOPIC_CONFIRMS = 'sdt/confirms/1.0.0';
export const TOPIC_COMMITS = 'sdt/commits/1.0.0';

export const LEADER_PORT = 9001;
export const LEADER_MULTIADDR =
  process.env.LEADER_MULTIADDR || `/ip4/127.0.0.1/tcp/${LEADER_PORT}`;
