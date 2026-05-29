import { describe, expect, it } from 'vitest';

import { buildIngestionSourceState } from '@app/context/import/buildIngestionSourceState';

describe('buildIngestionSourceState', () => {
    it('parses recipe yaml into builder state', () => {
        const state = buildIngestionSourceState({
            sourceType: 'github-documents',
            displayName: 'GitHub',
            recipeYaml: `source:
  type: github-documents
  config:
    repository: acme/docs
sink:
  type: datahub-rest
  config:
    server: "\${DATAHUB_GMS_URL}"
`,
        });

        expect(state.type).toBe('github-documents');
        expect(state.name).toBe('GitHub');
        expect(JSON.parse(state.config!.recipe!)).toEqual({
            source: {
                type: 'github-documents',
                config: {
                    repository: 'acme/docs',
                },
            },
            sink: {
                type: 'datahub-rest',
                config: {
                    server: '${DATAHUB_GMS_URL}',
                },
            },
        });
    });
});
