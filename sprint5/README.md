# Sprint 5 — Commit por Maioria, Indexação Vetorial e Deteção de Falha do Líder

Node.js + libp2p (GossipSub + Kademlia DHT) + IPFS (Kubo) + embeddings offline (`@huggingface/transformers`) + índice vetorial em memória (`hnswlib-node`, equivalente ao FAISS pedido no enunciado).

Esta pasta acumula os Sprints 1, 2 e 3, e fecha o **RF1** com o card "Atualização do vetor em todos os peers" (líder agrega maioria + commit; peers substituem versão e indexam) e implementa o **RNF3 (Parte 1)**: deteção de falha do líder por heartbeat.

## Pré-requisitos

- Node.js ≥ 18 (testado em v22.17.1)
- IPFS a correr localmente com API HTTP em `http://127.0.0.1:5001` (IPFS Desktop ou `ipfs daemon`)
- **Python 3** instalado e no PATH (necessário para compilar o `hnswlib-node`, que usa `node-gyp`)
- **Visual Studio Build Tools 2022**, com o workload "Desktop development with C++" **e** um componente **Windows SDK** instalado (ex: Windows 11 SDK 10.0.26100.0) — necessário para o `node-gyp` compilar o addon nativo do `hnswlib-node` no Windows

Em Linux/macOS normalmente basta ter as build-essentials/Xcode Command Line Tools instaladas; o processo é mais simples do que no Windows.

## Instalação

```bash
cd sprint5
npm install
```

Se o `npm install` falhar a compilar o `hnswlib-node` (erro de `node-gyp`/Visual Studio/Python em falta), consulta a secção **"Problemas conhecidos na instalação (Windows)"** mais abaixo — foi um obstáculo real que enfrentámos e documentámos a solução passo a passo.

## Como correr

**Terminal 1 — líder:**
```bash
npm run leader
```
Espera até veres `[Leader] API REST pronta` antes de arrancar os peers (o arranque do nó libp2p pode demorar alguns segundos).

**Terminal 2 e 3 — peers:**
```powershell
$env:PEER_LABEL="peer1"; node src/peer.js
$env:PEER_LABEL="peer2"; node src/peer.js
```

**Terminal 4 — submeter ficheiros:**
```bash
curl -F "file=@documento1.txt" http://localhost:8000/upload
curl -F "file=@documento2.txt" http://localhost:8000/upload
```

## O que mudou desde o Sprint 3

- **`src/protocol.js`**: novo tópico `TOPIC_COMMITS` (`sdt/commits/1.0.0`).
- **`src/leader.js`**:
  - `confirmedVersion`/`confirmedCids` passam a ser realmente atualizados (deixam de ficar vazios para sempre);
  - `estimatePeerCount(node)`: aproxima o número de peers ativos combinando `getSubscribers()` do GossipSub com `getPeers()` do libp2p (ligações diretas) — ver secção "Desafio do card" abaixo;
  - ao receber confirms válidos suficientes para atingir maioria (`Math.floor(peerCount/2)+1`), chama `commitVersion()`, que publica o commit no tópico `sdt/commits/1.0.0` e substitui a versão confirmada localmente.
- **`src/peer.js`**:
  - subscreve `sdt/commits/1.0.0`;
  - `handleCommit()`: valida sequência, substitui `confirmedVersion`/`confirmedCids`, move os embeddings pendentes para o índice `hnswlib-node` (criado com `cosine` como métrica, já que os embeddings são normalizados) e mantém um mapeamento `label -> CID`;
  - **RNF3 (Parte 1)**: cada broadcast periódico do líder (`sdt/broadcast/1.0.0`, a cada 5s) atualiza `lastHeartbeatTs`; um monitor (`monitorLeaderHeartbeat`, verificação a cada 1s) compara o tempo decorrido com `HEARTBEAT_TIMEOUT_MS` (12s por omissão) e regista `FALHA DO LÍDER DETETADA` quando o limite é ultrapassado. Se o líder voltar a responder, a falha é marcada como recuperada.

## RNF3 (Parte 1) — Deteção de falha do líder

Considera apenas falhas **fail-stop** (o líder para e não recupera sozinho) — conforme o RNF3 e o card explicitamente pedem. Não há ainda re-eleição de líder nem recuperação automática de ficheiros pinned (isso é trabalho futuro, riscado no próprio card).

Configuração via variável de ambiente (nos peers):

| Variável | Omissão | Descrição |
|---|---|---|
| `HEARTBEAT_TIMEOUT_MS` | `12000` (12s) | Tempo sem heartbeat até declarar falha do líder (n segundos do critério de aceitação) |

Testado manualmente: com o líder e um peer a correr, o peer recebia heartbeats a cada 5s normalmente; ao terminar o processo do líder, o peer registou `FALHA DO LÍDER DETETADA: sem heartbeat há 13s (limite=12s)` cerca de 13 segundos depois — dentro do limite configurado, com a margem de 1s da granularidade da verificação.

## Desafio do card: estimar o número de peers com desacoplamento espacial

O próprio card levanta a questão: *"como obter o número aproximado de peers, tendo em conta que existe desacoplamento espacial?"*

Testámos inicialmente só com `pubsub.getSubscribers(topic)` (GossipSub) — e **reproduzimos exatamente o problema que o card antecipa**: logo a seguir aos peers se ligarem, o líder viu `0` subscritores no momento do primeiro confirm (a informação de subscrição do GossipSub ainda não se tinha propagado), o que fez a maioria cair para `1` peer e disparou o commit **antes** do segundo peer conseguir responder.

Solução adotada: combinar `getSubscribers()` com `node.getPeers()` do libp2p (peers com ligação direta estabelecida) e usar o maior dos dois valores. Como nesta topologia todos os peers dialam o líder explicitamente, as ligações diretas são um sinal mais imediato e fiável do que a propagação de subscrições do pub/sub. Ainda assim, é uma **aproximação** — não uma contagem centralizada garantida — e fica documentado como tal no código.

## Cards e critérios de aceitação validados

### Sprint 5 — Atualização do vetor em todos os peers
- [x] Líder, após receber a maioria das respostas dos peers com hash correta, envia um commit para todos os peers
- [x] Líder substitui a versão atual do vetor de CIDs pela nova versão
- [x] Peer recebe o commit
- [x] Peer substitui a versão atual do vetor de CIDs pela nova versão
- [x] Peer atualiza a indexação (hnswlib, em memória)

Testado com 2 submissões sequenciais (v1 e v2): ambos os peers exigiram confirmação de **ambos** antes do commit (2/2), aplicaram os commits pela ordem correta e indexaram os embeddings com labels sequenciais (0, 1).

### RNF3 (Parte 1) — Deteção da falha do líder
- [x] Sistema considera apenas falhas do tipo fail-stop
- [x] Após terminar o processo do líder, os peers detetam a falha após n segundos (testado: ~13s com limite de 12s)

## Problemas conhecidos na instalação (Windows)

A instalação do `hnswlib-node` (dependência nativa, compilada com `node-gyp`) revelou uma cadeia de obstáculos reais neste ambiente, documentados aqui para referência do grupo:

1. **Falta de Python** → instalar Python 3 (via [python.org](https://www.python.org/downloads/), não só a Microsoft Store — o "stub" da Store não conta como instalação real e intercepta o comando `python`).
2. **Falta do Visual Studio Build Tools com Windows SDK** → no Visual Studio Installer, modificar o "Build Tools 2022", garantir o workload "Desktop development with C++" e adicionar um componente "Windows SDK" (ex: Windows 11 SDK 10.0.26100.0).
3. **`node-gyp` embutido no npm é antigo e incompatível com Python 3.12+** (usa `distutils`, removido do standard library) → usar uma versão mais recente do `node-gyp` (`npx node-gyp` costuma ir buscar a última versão) em vez do que vem empacotado com o npm.
4. Se o `npm install` completo falhar por causa disto, uma alternativa é instalar com `npm install --ignore-scripts` (instala tudo sem compilar nada) e depois compilar só o `hnswlib-node` manualmente:
   ```bash
   npm install --ignore-scripts
   cd node_modules/hnswlib-node
   npx node-gyp rebuild --python="C:\Caminho\Para\python.exe"
   ```

Dado o esforço necessário só para conseguir compilar esta dependência, consideramos esta uma limitação prática relevante do FAISS/hnswlib em Node.js no Windows, e vale a pena referir na conclusão do relatório.

## Limitações conhecidas nesta fase

- O líder é estático (sem eleição) — RNF4 fica para trabalho futuro.
- A resolução de conflitos de versão continua "a implementar no futuro" (herdado do Sprint 3) — um peer que perca uma proposta/commit fica bloqueado até reiniciar.
- A capacidade do índice hnswlib está fixada em 10000 elementos (`HNSW_MAX_ELEMENTS`); não há redimensionamento dinâmico automático.
- A deteção de falha do líder (RNF3 Parte 1) é apenas deteção/log - não há re-eleição de líder (RNF4) nem re-pinning automático de ficheiros em caso de falha de peer (RNF3 Parte 2), ambos fora de âmbito nesta pasta.
