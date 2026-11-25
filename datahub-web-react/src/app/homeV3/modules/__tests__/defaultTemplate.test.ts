import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';
import YAML from 'yamljs';

import { DEFAULT_TEMPLATE, DEFAULT_TEMPLATE_URN } from '@app/homeV3/modules/constants';

type SeededTemplate = { entityUrn: string; aspect: { rows: { modules: string[] }[] } };

// The template GMS seeds on new instances; the client-side fallback should render the same rows.
function seededDefaultTemplateRows(): string[][] {
    const raw = fs.readFileSync(
        path.resolve(
            process.cwd(),
            '../metadata-service/configuration/src/main/resources/bootstrap_mcps/page-templates.yaml',
        ),
        'utf-8',
    );
    // Mustache placeholders such as {{&auditStamp}} are not valid YAML values.
    const templates: SeededTemplate[] = YAML.parse(raw.replace(/\{\{[^}]*\}\}/g, 'null'));
    const seeded = templates.find((template) => template.entityUrn === DEFAULT_TEMPLATE_URN);
    return seeded?.aspect.rows.map((row) => row.modules) ?? [];
}

describe('DEFAULT_TEMPLATE', () => {
    it('has the same rows as the home page template seeded by bootstrap', () => {
        const fallbackRows = DEFAULT_TEMPLATE.properties?.rows.map((row) => row.modules.map((module) => module.urn));

        expect(fallbackRows).toEqual(seededDefaultTemplateRows());
    });
});
