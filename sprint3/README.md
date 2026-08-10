# Sprint 3 — Confirmação do Vetor de Documentos pelos Peers

Node.js + libp2p (GossipSub + Kademlia DHT) + IPFS (Kubo) + embeddings offline (`@huggingface/transformers`).

Esta pasta acumula os Sprints 1 e 2 (instalação/uso do IPFS, API REST, routing entre peers, geração/propagação de embeddings) e adiciona o card do **Sprint 3**: verificação de conflitos de versão, hash do vetor de CIDs e confirmação enviada ao líder.

## Pré-requisitos

- Node.js ≥ 18 (testado em v22.17.1)
- IPFS a correr localmente com API HTTP acessível em `http://127.0.0.1:5001` (IPFS Desktop aberto, ou `ipfs daemon`)

## Instalação

```bash
cd sprint3
npm install
```

## Como correr

**Terminal 1 — líder:**
```bash
npm run leader
```

**Terminal 2 e 3 — peers:**
```powershell
$env:PEER_LABEL="peer1"; node src/peer.js
$env:PEER_LABEL="peer2"; node src/peer.js
```

**Terminal 4 — submeter um ficheiro:**
```bash
curl -F "file=@documento.txt" http://localhost:8000/upload
```

## Variáveis de ambiente

Iguais ao Sprint 2: `LEADER_HTTP_PORT`, `IPFS_API_URL`, `LEADER_MULTIADDR`, `PEER_LABEL`.

## O que mudou desde o Sprint 2

- **`src/protocol.js`**: novo tópico `TOPIC_CONFIRMS` (`sdt/confirms/1.0.0`).
- **`src/peer.js`**:
  - ao receber uma proposta, verifica se a `version` é exatamente a próxima esperada (`confirmedVersion + 1`); caso contrário, regista **conflito** e ignora a proposta (resolução de conflitos fica "a implementar no futuro", conforme o card);
  - se não houver conflito, guarda a nova versão do vetor + embeddings temporariamente (`Map pending`);
  - calcula a hash SHA-256 do vetor de CIDs e publica-a no tópico `sdt/confirms/1.0.0`;
  - liga-se ao líder com **retry** (até 5 tentativas, 2s de intervalo), para tolerar arranques assíncronos entre processos.
- **`src/leader.js`**: subscreve `sdt/confirms/1.0.0`, regista os hashes recebidos por peer/versão (`Map confirmations`) e valida-os contra o hash local do vetor pendente atual. A lógica de maioria + commit fica formalmente para o **Sprint 5** (o card permite isso explicitamente).

## Cards e critérios de aceitação validados

### Sprint 3 — Atualização do vetor de documentos e confirmação
- [x] Cliente submete ficheiro através da API do líder
- [x] Cada peer, após receber o pedido de atualização, armazena a nova versão do vetor e os embeddings em estruturas temporárias
- [x] Após o processamento pelos peers, os hashes são enviados ao líder (e validados)

## Comportamento importante a saber (para os testes/demo)

Como o **commit ainda não existe** nesta fase (é Sprint 5), a `confirmedVersion` de cada peer nunca avança além de `0`. Isto significa que:

- A **primeira** submissão de sempre (v1) é sempre aceite por todos os peers.
- Qualquer submissão **seguinte** (v2, v3, ...) vai gerar **conflito de versão** em todos os peers, propositadamente — porque cada peer continua à espera de "v1" como próxima versão válida, já que nunca recebeu um commit que a confirmasse.
- Isto **não é um bug**: é exatamente o comportamento esperado do card, e serve como prova de que a deteção de conflitos funciona. Só a partir do Sprint 5 (commit por maioria) é que os peers passam a avançar de versão em versão.

Os avisos de conflito vão para o `stderr` do processo (`console.warn`), não para o `stdout` — se estiveres a redirecionar os dois separadamente (como nos scripts de teste), verifica o `*.err.log`.

## Limitações conhecidas nesta fase (a resolver no Sprint 5)

- Sem commit/consenso de maioria: a versão nunca é "confirmada" de facto, o que limita cada peer a aceitar apenas uma versão por execução (ver secção acima).
- Sem indexação FAISS/hnswlib definitiva (só armazenamento temporário dos embeddings).
- O líder é estático (sem eleição).
