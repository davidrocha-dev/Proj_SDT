# Entrega Final — Sistema de Armazenamento Distribuído P2P

Node.js + libp2p (GossipSub + Kademlia DHT) + IPFS (Kubo) + embeddings offline (`@huggingface/transformers`) + índice vetorial em memória (`hnswlib-node`, equivalente ao FAISS pedido no enunciado).

Esta pasta é a versão consolidada de todo o trabalho (Sprints 1, 2, 3 e 5), correspondendo ao código final do `sprint5/`. Para a análise completa da arquitetura, decisões de implementação e conclusões, ver **[relatorio.md](relatorio.md)**.

## Pré-requisitos

- Node.js ≥ 18 (testado em v22.17.1)
- IPFS a correr localmente com API HTTP em `http://127.0.0.1:5001` (IPFS Desktop ou `ipfs daemon`)
- **Python 3** instalado e no PATH (necessário para compilar o `hnswlib-node`, que usa `node-gyp`)
- **Visual Studio Build Tools 2022**, com o workload "Desktop development with C++" **e** um componente **Windows SDK** instalado (ex: Windows 11 SDK 10.0.26100.0) — necessário para o `node-gyp` compilar o addon nativo do `hnswlib-node` no Windows

Em Linux/macOS normalmente basta ter as build-essentials/Xcode Command Line Tools instaladas; o processo é mais simples do que no Windows.

## Instalação

```bash
cd final
npm install
```

Se o `npm install` falhar a compilar o `hnswlib-node` (erro de `node-gyp`/Visual Studio/Python em falta), consulta a secção **"Problemas conhecidos na instalação (Windows)"** mais abaixo.

## Como correr

**Terminal 1 — líder:**
```bash
npm run leader
```
Espera até veres `[Leader] API REST pronta` antes de arrancar os peers (o arranque do nó libp2p pode demorar alguns segundos).

**Terminal 2 e 3 (ou mais) — peers:**
```powershell
$env:PEER_LABEL="peer1"; node src/peer.js
$env:PEER_LABEL="peer2"; node src/peer.js
```
```bash
PEER_LABEL=peer1 node src/peer.js
PEER_LABEL=peer2 node src/peer.js
```

**Terminal seguinte — submeter ficheiros:**
```bash
curl -F "file=@documento1.txt" http://localhost:8000/upload
curl -F "file=@documento2.txt" http://localhost:8000/upload
```

## Variáveis de ambiente

| Variável | Omissão | Descrição |
|---|---|---|
| `LEADER_HTTP_PORT` | `8000` | Porta da API REST do líder |
| `IPFS_API_URL` | `http://127.0.0.1:5001` | Endpoint da API do Kubo |
| `LEADER_MULTIADDR` | `/ip4/127.0.0.1/tcp/9001` | Endereço libp2p do líder, usado pelos peers para se ligarem |
| `PEER_LABEL` | `peer-<pid>` | Rótulo do peer nos logs |
| `HEARTBEAT_TIMEOUT_MS` | `12000` (12s) | Tempo sem heartbeat do líder até um peer declarar falha |

Para testar entre máquinas diferentes, arranca o líder numa delas e define `LEADER_MULTIADDR` nos peers com o IP dessa máquina, ex: `/ip4/192.168.1.34/tcp/9001`.

## Estrutura do código

- `src/libp2p-node.js` — factory partilhada do nó libp2p (TCP, noise, yamux, identify, ping, GossipSub, Kademlia DHT).
- `src/protocol.js` — constantes do protocolo (4 tópicos GossipSub, endereço do líder).
- `src/leader.js` — API REST, IPFS, embeddings, vetor de CIDs, propagação, agregação de confirmações, commit por maioria, heartbeat.
- `src/peer.js` — routing, deteção de conflitos, confirmação por hash, aplicação de commits, indexação hnswlib, deteção de falha do líder.
- `src/test-*.js` — scripts de validação isolada (IPFS, libp2p, embeddings, hnswlib), não fazem parte do fluxo de produção.
- `relatorio.md` — relatório técnico (Introdução, Arquitetura UML, Implementação, Conclusão).

## Requisitos cobertos nesta entrega

### RF1 — Armazenar Ficheiros
Implementado por completo, distribuído pelos Sprints 1, 2, 3 e 5:
- Instalação/uso do IPFS, API REST de submissão, routing entre peers via libp2p (Sprint 1)
- Geração de embeddings e propagação do vetor de documentos (Sprint 2)
- Verificação de conflitos de versão, hash e confirmação dos peers (Sprint 3)
- Commit por maioria, substituição de versão e indexação vetorial (Sprint 5)

### RNF3 (Parte 1) — Tolerância a Falhas (deteção)
- Sistema considera apenas falhas fail-stop
- Peers detetam a falha do líder após *n* segundos sem heartbeat (testado: ~13s com limite de 12s)

### Fora de âmbito nesta entrega
RF2 (Pesquisa de Informação), RNF3 Parte 2 (recuperação automática de pinning), RNF4 (eleição de líder), RNF5 (dinamicidade de peers) e RNF6 (segurança) — ver secção "Possíveis melhorias e funcionalidades futuras" do relatório.

## Desafio do card (Sprint 5): estimar o número de peers com desacoplamento espacial

Resolvido combinando `pubsub.getSubscribers(topic)` (GossipSub) com `node.getPeers()` (ligações diretas do libp2p), usando o maior dos dois valores. Detalhe completo, incluindo o problema real que reproduzimos antes de corrigir, no `relatorio.md` (secção 3.5).

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

## Segurança das dependências

Todas as dependências foram verificadas com `npm audit` — o projeto termina com **0 vulnerabilidades** reportadas. Duas trocas relevantes feitas durante o desenvolvimento: `@xenova/transformers` → `@huggingface/transformers` (sucessor mantido, sem vulnerabilidades críticas herdadas) e `multer` 1.x → 2.x.

## Limitações conhecidas

Ver secção "4.1 Limitações da solução atual" em `relatorio.md` para a lista completa e justificada.
