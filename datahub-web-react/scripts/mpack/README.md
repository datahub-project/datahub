# mpack: a sparse dev loader for remote development

`mpack` sits in front of the Vite dev server and makes the web app fast to reload when your browser is far
from the dev server: a remote dev box, a cloud workstation, a VPN or an SSH/SSM port-forward.

Vite's dev server serves every source module as its own request: several thousand for DataHub's home page.
The browser only discovers a module's imports after downloading it, so a page load is a long waterfall of
sequential round trips. Locally that costs little; over a link with ~100 ms round trips it takes minutes, and
reloads pay it again because Vite revalidates every module. HTTP/2 does not fix this (the waterfall is in the
import graph, not the connection limit).

mpack keeps Vite unbundled and adds a service worker that holds the modules in the browser:

- **Server** (`server.mjs`): reads modules from Vite once, strips the inline source maps, gzips them and cuts the
  load-ordered list into content-addressed chunks (each named by the hash of its modules' hashes; a Merkle root
  over the chunks names the whole pack). Chunk boundaries are content-defined (they depend on module paths), so
  editing one module changes one chunk. Chunks are served `immutable`; everything else passes through to Vite.
- **Service worker** (`sw.js`): on each page load asks for the root (one tiny request). If it changed, it fetches
  the chunk index and downloads only the chunks it does not have, then answers every module request from the
  cache. Module requests wait for that update, so an edited module is never served stale.
- **Live reload**: mpack follows Vite's HMR socket. When Vite reports a change it re-reads only the changed
  modules and their importers, rebuilds the chunks (milliseconds), and tells open pages to reload once.
- **Learning**: modules the page fetches that mpack does not have yet are learned and packed for the next load,
  so no module list is needed (`--modules` can seed one).

It is out of tree on purpose: no app code depends on it and nothing changes when it is not running.

## Use

```bash
yarn start                                  # Vite dev server on :3000, as usual
node scripts/mpack/cli.mjs --vite http://127.0.0.1:3000 --port 3010
```

Open `http://<host>:3010/` (port-forward 3010 instead of 3000). The first visit registers the worker and fills
its cache; after that, reloads and edits are served from it. Node 22+ (it uses the built-in `WebSocket`).

Options: `--port` (default 3010), `--work-dir` (chunk files; default a `mpack` dir under the OS temp dir),
`--modules` (optional JSON list of module URLs to pack up front).

## What to expect

Measured on a DataHub build (Vite 6, ~2,200 modules on the login and home pages), browser on a laptop,
dev server on a cloud machine reached through an SSM port-forward (~80–100 ms round trips, ~6 Mbit/s):

|                     | Vite dev directly | through mpack                                  |
| ------------------- | ----------------- | ---------------------------------------------- |
| first load          | 31 s              | 13.6 s (fills the cache once)                  |
| reload              | 7.7 s             | 1.4–1.7 s                                      |
| edit → page updated | reload by hand    | ~1.9 s (reloads itself; ~25 ms of it is mpack) |

On the same machine as the dev server there is little to win: reloads are about as fast as plain Vite.

## Limits

- Full-page reloads only: Vite's hot module replacement is not proxied (pages reload instead).
- Source maps are stripped from packed modules (smaller over slow links); debug against Vite directly if you
  need them.
- The browser's resource-timing buffer, extensions and devtools "Disable cache" are unaffected; to bypass mpack,
  unregister the service worker in devtools (Application → Service workers) or use the Vite port.

## Tests

```bash
node --test --experimental-test-coverage --test-coverage-include='scripts/mpack/*' \
  --test-coverage-exclude='scripts/mpack/*.test.mjs' --test-coverage-lines=100 \
  --test-coverage-branches=100 --test-coverage-functions=100 scripts/mpack/*.test.mjs
```
