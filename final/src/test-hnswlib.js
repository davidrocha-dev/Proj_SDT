// Teste rápido: confirma que o hnswlib-node compilou e funciona (índice vetorial,
// adição de pontos e pesquisa por similaridade) antes de integrar no peer.js.

import hnswlibPkg from 'hnswlib-node';
const { HierarchicalNSW } = hnswlibPkg;

async function main() {
  const dim = 4;
  const index = new HierarchicalNSW('cosine', dim);
  index.initIndex(10);

  index.addPoint([1, 0, 0, 0], 0);
  index.addPoint([0, 1, 0, 0], 1);
  index.addPoint([0.9, 0.1, 0, 0], 2);

  const result = index.searchKnn([1, 0, 0, 0], 2);
  console.log('[test-hnswlib] Vizinhos mais próximos de [1,0,0,0]:', result);
  console.log('[test-hnswlib] OK: índice funcional.');
}

main().catch((err) => {
  console.error('[test-hnswlib] Erro:', err);
  process.exit(1);
});
