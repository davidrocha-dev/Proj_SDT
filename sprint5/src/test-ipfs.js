// Teste rápido: confirma que o Node.js consegue falar com o Kubo local (IPFS Desktop)
// via kubo-rpc-client, adicionando e obtendo um ficheiro. Substitui a verificação
// manual do critério de aceitação do Sprint 1 ("peers conseguem adicionar/obter ficheiros").

import { create } from 'kubo-rpc-client';

const ipfs = create({ url: 'http://127.0.0.1:5001' });

async function main() {
  const content = `Ficheiro de teste gerado em ${new Date().toISOString()}`;

  console.log('[test-ipfs] A adicionar ficheiro...');
  const { cid } = await ipfs.add(content);
  console.log(`[test-ipfs] CID gerado: ${cid.toString()}`);

  console.log('[test-ipfs] A obter o ficheiro pelo CID...');
  const chunks = [];
  for await (const chunk of ipfs.cat(cid)) {
    chunks.push(chunk);
  }
  const retrieved = Buffer.concat(chunks).toString('utf-8');

  console.log(`[test-ipfs] Conteúdo obtido: "${retrieved}"`);
  console.log(retrieved === content ? '[test-ipfs] OK: conteúdo coincide.' : '[test-ipfs] FALHOU: conteúdo diferente.');
}

main().catch((err) => {
  console.error('[test-ipfs] Erro:', err);
  process.exit(1);
});
