import * as fs from 'fs';
import * as path from 'path';
import type { Plugin } from 'vite';
import { brotliCompressSync, gzipSync, constants as zlibConstants } from 'zlib';

/** Skip tiny files — gzip/br framing can make them larger than the original. */
export const MIN_PRECOMPRESS_BYTES = 1024;

const COMPRESSIBLE_EXTENSIONS = new Set([
    '.js',
    '.mjs',
    '.cjs',
    '.css',
    '.html',
    '.json',
    '.svg',
    '.txt',
    '.xml',
    '.wasm',
    '.map',
]);

export type PrecompressStats = {
    files: number;
    gzipBytes: number;
    brotliBytes: number;
    originalBytes: number;
};

export function shouldPrecompressFile(filePath: string, sizeBytes: number): boolean {
    if (sizeBytes < MIN_PRECOMPRESS_BYTES) {
        return false;
    }
    if (filePath.endsWith('.gz') || filePath.endsWith('.br')) {
        return false;
    }
    return COMPRESSIBLE_EXTENSIONS.has(path.extname(filePath).toLowerCase());
}

function gzipBuffer(source: Buffer): Buffer {
    return gzipSync(source, { level: zlibConstants.Z_BEST_COMPRESSION });
}

function brotliBuffer(source: Buffer): Buffer {
    return brotliCompressSync(source, {
        params: {
            [zlibConstants.BROTLI_PARAM_QUALITY]: zlibConstants.BROTLI_MAX_QUALITY,
            [zlibConstants.BROTLI_PARAM_SIZE_HINT]: source.length,
        },
    });
}

function writeSidecarIfSmaller(sidecarPath: string, compressed: Buffer, originalSize: number): number {
    if (compressed.length >= originalSize) {
        return originalSize;
    }
    fs.writeFileSync(sidecarPath, compressed);
    return compressed.length;
}

function listFilesRecursive(rootDir: string): string[] {
    return fs.readdirSync(rootDir, { withFileTypes: true }).flatMap((entry) => {
        const fullPath = path.join(rootDir, entry.name);
        if (entry.isDirectory()) {
            return listFilesRecursive(fullPath);
        }
        return entry.isFile() ? [fullPath] : [];
    });
}

export function precompressDirectory(rootDir: string): PrecompressStats {
    const stats: PrecompressStats = { files: 0, gzipBytes: 0, brotliBytes: 0, originalBytes: 0 };

    listFilesRecursive(rootDir).forEach((fullPath) => {
        const sizeBytes = fs.statSync(fullPath).size;
        if (!shouldPrecompressFile(fullPath, sizeBytes)) {
            return;
        }
        const source = fs.readFileSync(fullPath);
        const gzipBytes = writeSidecarIfSmaller(`${fullPath}.gz`, gzipBuffer(source), source.length);
        const brotliBytes = writeSidecarIfSmaller(`${fullPath}.br`, brotliBuffer(source), source.length);
        stats.files += 1;
        stats.originalBytes += source.length;
        stats.gzipBytes += gzipBytes;
        stats.brotliBytes += brotliBytes;
    });

    return stats;
}

/**
 * Writes `.br` (quality 11) and `.gz` (level 9) sidecars next to compressible
 * files in the Vite output directory. Play's Assets controller serves them
 * based on Accept-Encoding — no per-request compression of hashed bundles.
 *
 * `order: 'post'` so this runs after vite-plugin-static-copy has placed Monaco
 * and other copied assets into dist.
 */
export function precompressAssetsPlugin(): Plugin {
    let outDir = '';

    return {
        name: 'precompress-assets',
        apply: 'build',
        configResolved(config) {
            outDir = path.resolve(config.root, config.build.outDir);
        },
        closeBundle: {
            sequential: true,
            order: 'post',
            handler() {
                if (!outDir || !fs.existsSync(outDir)) {
                    return;
                }
                const stats = precompressDirectory(outDir);
                if (stats.files === 0) {
                    return;
                }
                const gzipPct = stats.originalBytes === 0 ? 0 : (1 - stats.gzipBytes / stats.originalBytes) * 100;
                const brotliPct = stats.originalBytes === 0 ? 0 : (1 - stats.brotliBytes / stats.originalBytes) * 100;
                const vsGzip =
                    stats.gzipBytes === 0 ? 0 : ((stats.gzipBytes - stats.brotliBytes) / stats.gzipBytes) * 100;
                console.log(
                    `[precompress] ${stats.files} files: ` +
                        `gzip ${gzipPct.toFixed(1)}% smaller, ` +
                        `brotli ${brotliPct.toFixed(1)}% smaller ` +
                        `(${vsGzip.toFixed(1)}% vs gzip)`,
                );
            },
        },
    };
}
