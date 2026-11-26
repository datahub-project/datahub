import { defineConfig } from 'vitest/config';

// Server middleware needs Node's HTTP and encoding primitives, without React's browser setup.
export default defineConfig({
    cacheDir: 'node_modules/.vite-devtools-tests',
    test: { environment: 'node', include: ['devtools/**/__tests__/*.node.test.ts'], maxWorkers: 2, minWorkers: 1 },
});
