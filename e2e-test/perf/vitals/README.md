# Boot web-vitals A/B

Lighthouse comparison of two production frontend builds. Frontend Preview uses it for an unauthenticated `/login` comment. There is no GMS. The comment does not replace the gzip table and does not gate the pull request.

CI serves the two `dist/` trees copied by the bundle-size step (`config.ci.json`: Lantern, desktop, 1× CPU, 3 measured runs, 1 warm-up). Pushes to `master` measure the head build only. The JSON artifact is `vitals-boot-pr-<n>` or `vitals-boot-master`.

Local unit tests do not download Chrome:

```bash
cd e2e-test/perf/vitals
PUPPETEER_SKIP_DOWNLOAD=true npm ci
npm test
```

To compare two local builds, copy `config.example.json` to `config.json`, point `builds.A.dist` and `builds.B.dist` at the two `dist/` directories, then:

```bash
npm ci
node lh-ab.mjs --config config.json --no-auth --out results/report.json --md-out results/report.md
```

Runs alternate A, B, A, B. A batched all-A-then-all-B loop lets host drift land on whichever build ran second. The comment is a lab delta for this runner, not a fleet p75.

## Eager bundle inventory

`bundle-inventory.mjs` attributes eager `index.html` JS (same set as
`datahub-web-react/scripts/eager-budget.mjs`) to packages via source maps.

`frontend-build` runs `--regression` whenever the eager budget has soft or hard
findings. That report diffs the fresh `dist/` against
`datahub-web-react/eager-budget-inventory.json` and appends markdown to the job
summary. The weekly `Bundle budget reassessment` workflow runs the same report
on `master` and opens a PR only to ratchet baselines down.

When you change `eager-budget.json`, refresh the inventory from the same build:

```bash
node e2e-test/perf/vitals/bundle-inventory.mjs \
  --dist datahub-web-react/dist \
  --write-baseline datahub-web-react/eager-budget-inventory.json
```
