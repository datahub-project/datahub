# Boot web-vitals A/B

Lighthouse comparison of two production frontend builds. Frontend Preview posts one comment, `<!-- web-vitals-boot-ab -->`, with two routes and no GMS:

- `/login` is logged out, on Lantern at 1× CPU. It measures the eager bundle.
- `/` is the signed-in shell. The lab sets an `actor` cookie and serves recorded GraphQL over HTTP/1.1. Throttling is devtools at CPU 4×, so main-thread parse shows up. It does not measure a live search or dataset payload.

Both use 2 warm-ups and 5 measured runs, alternating A,B. The comment judges first paint and LCP separately. Noise means the A and B ranges overlap. No change means a separated median under 5% and under 50 ms. A route is mixed when those two metrics move in opposite directions. Noise and no change stay on one line. A regression, an improvement, a mixed result, or a short sample keeps that route's table. The comment does not replace the gzip table and does not gate the pull request.

`config.ci.json` holds the login pass. `run-ci-boot.mjs` derives the home pass from it. Pushes to `master` measure the head build only. The JSON artifact is `vitals-boot-pr-<n>` or `vitals-boot-master` (`results/ci-boot.json`, with `login` and `home`).

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
