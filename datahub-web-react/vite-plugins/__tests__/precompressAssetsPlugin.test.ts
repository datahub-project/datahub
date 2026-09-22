import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import { brotliDecompressSync, gunzipSync } from 'zlib';

/* Vite plugin lives outside `src/` so Node can load it without path aliases. */
/* eslint-disable import-alias/import-alias, import/extensions */
import { MIN_PRECOMPRESS_BYTES, precompressDirectory, shouldPrecompressFile } from '../precompressAssetsPlugin';

/* eslint-enable import-alias/import-alias, import/extensions */

describe('precompressAssetsPlugin', () => {
    const tmpDirs: string[] = [];

    afterEach(() => {
        tmpDirs.forEach((dir) => {
            fs.rmSync(dir, { recursive: true, force: true });
        });
        tmpDirs.length = 0;
    });

    function tmpDir(): string {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'precompress-'));
        tmpDirs.push(dir);
        return dir;
    }

    it('skips tiny files, images, and existing sidecars', () => {
        expect(shouldPrecompressFile('chunk.js', MIN_PRECOMPRESS_BYTES - 1)).toBe(false);
        expect(shouldPrecompressFile('logo.png', 50_000)).toBe(false);
        expect(shouldPrecompressFile('chunk.js.gz', 50_000)).toBe(false);
        expect(shouldPrecompressFile('chunk.js.br', 50_000)).toBe(false);
        expect(shouldPrecompressFile('chunk.js', MIN_PRECOMPRESS_BYTES)).toBe(true);
        expect(shouldPrecompressFile('styles.css', MIN_PRECOMPRESS_BYTES)).toBe(true);
    });

    it('writes gzip and brotli sidecars that round-trip and beat gzip on JS', () => {
        const dir = tmpDir();
        const assets = path.join(dir, 'assets');
        fs.mkdirSync(assets);

        // Repeating minified-like JS compresses well; size is well above the threshold.
        const source = `export function x(){return "${'payload-'.repeat(400)}";}\n`;
        const filePath = path.join(assets, 'source-By3zWfCx.js');
        fs.writeFileSync(filePath, source);

        const tiny = path.join(assets, 'tiny.js');
        fs.writeFileSync(tiny, 'export default 1;\n');

        const png = path.join(assets, 'logo.png');
        fs.writeFileSync(png, Buffer.alloc(2048, 1));

        const stats = precompressDirectory(dir);

        expect(stats.files).toBe(1);
        expect(fs.existsSync(`${filePath}.gz`)).toBe(true);
        expect(fs.existsSync(`${filePath}.br`)).toBe(true);
        expect(fs.existsSync(`${tiny}.gz`)).toBe(false);
        expect(fs.existsSync(`${png}.gz`)).toBe(false);

        const original = fs.readFileSync(filePath);
        const gzipped = fs.readFileSync(`${filePath}.gz`);
        const brotlied = fs.readFileSync(`${filePath}.br`);

        expect(gunzipSync(gzipped).equals(original)).toBe(true);
        expect(brotliDecompressSync(brotlied).equals(original)).toBe(true);

        expect(gzipped.length).toBeLessThan(original.length);
        expect(brotlied.length).toBeLessThan(gzipped.length);
        expect(stats.brotliBytes).toBeLessThan(stats.gzipBytes);

        const vsGzip = ((stats.gzipBytes - stats.brotliBytes) / stats.gzipBytes) * 100;
        // Ticket expectation is ~15-20% on real minified bundles; this synthetic
        // payload should still show a clear brotli win over gzip.
        expect(vsGzip).toBeGreaterThan(5);
    });
});
