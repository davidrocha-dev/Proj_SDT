// Teste rápido: confirma que conseguimos gerar embeddings offline em Node.js
// com @huggingface/transformers (modelo all-MiniLM-L6-v2), antes de integrar no leader.js.

import { pipeline } from '@huggingface/transformers';

async function main() {
  console.log('[test-embeddings] A carregar o modelo (pode demorar na primeira vez)...');
  const extractor = await pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');

  const text = 'Documento de teste para geração de embeddings.';
  const output = await extractor(text, { pooling: 'mean', normalize: true });

  const vector = Array.from(output.data);
  console.log(`[test-embeddings] Embedding gerado. Dimensão: ${vector.length}`);
  console.log(`[test-embeddings] Primeiros 5 valores: [${vector.slice(0, 5).map((v) => v.toFixed(4)).join(', ')}]`);
}

main().catch((err) => {
  console.error('[test-embeddings] Erro:', err);
  process.exit(1);
});
