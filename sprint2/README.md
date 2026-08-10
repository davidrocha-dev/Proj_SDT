# Sprint 2 — Armazenamento Distribuído de Ficheiros

Node.js + libp2p (GossipSub + Kademlia DHT) + IPFS (Kubo) + embeddings offline (`@huggingface/transformers`).

Esta pasta inclui a base do **Sprint 1** (instalação/uso do IPFS, API REST de submissão, routing entre peers) e o card do **Sprint 2** (atualização do vetor de documentos), já que os sprints se acumulam.

## Pré-requisitos

- Node.js ≥ 18 (testado em v22.17.1)
- IPFS a correr localmente com API HTTP acessível em `http://127.0.0.1:5001`:
  - **IPFS Desktop** aberto, ou
  - `ipfs daemon` (Kubo standalone) a correr no CLI
- Confirmar que a API responde: `curl http://127.0.0.1:5001/api/v0/version` (via POST)

## Instalação

```bash
cd sprint2
npm install
```

## Como correr

Precisas de pelo menos 3 terminais (1 líder + 2 peers), todos a partir da pasta `sprint2/`.

**Terminal 1 — líder:**
```bash
npm run leader
```

**Terminal 2 e 3 — peers** (o `PEER_LABEL` é opcional, só para diferenciar nos logs):

PowerShell:
```powershell
$env:PEER_LABEL="peer1"; node src/peer.js
```
```powershell
$env:PEER_LABEL="peer2"; node src/peer.js
```

Bash:
```bash
PEER_LABEL=peer1 node src/peer.js
PEER_LABEL=peer2 node src/peer.js
```

**Terminal 4 — submeter um ficheiro** (cliente):
```bash
curl -F "file=@documento.txt" http://localhost:8000/upload
```

## Variáveis de ambiente

| Variável | Omissão | Descrição |
|---|---|---|
| `LEADER_HTTP_PORT` | `8000` | Porta da API REST do líder |
| `IPFS_API_URL` | `http://127.0.0.1:5001` | Endpoint da API do Kubo |
| `LEADER_MULTIADDR` | `/ip4/127.0.0.1/tcp/9001` | Endereço libp2p do líder, usado pelos peers para se ligarem |
| `PEER_LABEL` | `peer-<pid>` | Rótulo do peer nos logs |

Para testar entre máquinas diferentes, arranca o líder numa delas e define `LEADER_MULTIADDR` nos peers com o IP dessa máquina, ex: `/ip4/192.168.1.34/tcp/9001`.

## Arquitetura resumida

- `src/libp2p-node.js` — factory partilhada do nó libp2p (TCP, noise, yamux, identify, ping, GossipSub, Kademlia DHT).
- `src/protocol.js` — constantes do protocolo (tópicos GossipSub, endereço do líder).
- `src/leader.js` — API REST (`POST /upload`), adiciona ficheiro ao IPFS, gera embeddings, mantém o vetor de CIDs pendente, propaga `[versão, CID, embeddings]` via GossipSub, e envia heartbeat de broadcast a cada 5s.
- `src/peer.js` — liga-se ao líder, subscreve os tópicos, guarda propostas recebidas temporariamente (`Map` em memória) e mostra tudo na consola.
- `src/test-*.js` — scripts avulsos usados para validar isoladamente o IPFS, o libp2p e as embeddings antes da integração (não fazem parte do fluxo de produção).

## Cards e critérios de aceitação validados

### Sprint 1 — Instalação do IPFS
- [x] IPFS instalado em cada peer (testado em 2 PCs distintos)
- [x] Peer consegue adicionar ficheiros
- [x] Qualquer peer consegue obter um ficheiro adicionado por outro peer

### Sprint 1 — Criação da API para submissão de um ficheiro
- [x] Cliente submete ficheiro através da API do líder (`POST /upload`)
- [x] Líder adiciona o ficheiro ao IPFS e devolve o CID

### Sprint 1 — Implementação do routing entre peers
- [x] Mensagem enviada pelo líder é visualizada na consola de cada peer (broadcast periódico via GossipSub)

### Sprint 2 — Atualização do vetor de documentos
- [x] Cliente submete ficheiro através da API do líder
- [x] Qualquer peer recebe o novo vetor de CIDs propagado pelo líder e os embeddings do ficheiro (via GossipSub, tópico de propostas)

## Limitações conhecidas nesta fase (a resolver em sprints seguintes)

- Não há ainda verificação de conflitos de versão, hash, nem confirmação por parte dos peers (Sprint 3).
- Não há commit/consenso de maioria nem indexação FAISS/hnswlib definitiva (Sprint 5).
- O líder é estático (sem eleição) — conforme permitido nesta fase do enunciado.
- Os primeiros broadcasts/propostas podem perder-se enquanto a malha GossipSub ainda está a formar-se (comportamento normal do protocolo, mitigado pelo reenvio periódico de heartbeat).
