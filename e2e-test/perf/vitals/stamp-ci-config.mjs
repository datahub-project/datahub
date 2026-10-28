#!/usr/bin/env node
import * as fs from 'fs';

const cfg = JSON.parse(fs.readFileSync('config.ci.json', 'utf8'));
const base = process.env.BASE_SHA || '';
const head = process.env.HEAD_SHA || process.env.GITHUB_SHA || '';
cfg.builds.A.commit = base || null;
cfg.builds.A.label = `base (${base.slice(0, 7) || 'unknown'})`;
cfg.builds.B.commit = head || null;
cfg.builds.B.label = `pr (${head.slice(0, 7) || 'unknown'})`;
fs.writeFileSync('config.ci.run.json', `${JSON.stringify(cfg, null, 2)}\n`);
