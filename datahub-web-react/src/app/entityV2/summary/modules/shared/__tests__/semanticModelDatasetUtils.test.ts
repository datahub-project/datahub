import { describe, expect, it } from 'vitest';

import {
    getSemanticModelDatasetDescription,
    getSemanticModelDatasetDisplayName,
    getSemanticModelDatasetLabel,
    withSemanticModelAlias,
} from '@app/entityV2/summary/modules/shared/semanticModelDatasetUtils';

import { Dataset, EntityType } from '@types';

function buildDataset(overrides: Partial<Dataset> = {}): Dataset {
    return {
        urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)',
        type: EntityType.Dataset,
        name: 'db.schema.table',
        ...overrides,
    } as Dataset;
}

describe('semanticModelDatasetUtils', () => {
    describe('getSemanticModelDatasetDisplayName', () => {
        it('prefers editableProperties.name over properties.name', () => {
            const dataset = buildDataset({
                name: 'raw.name',
                properties: { name: 'Logical Name' } as Dataset['properties'],
                editableProperties: { name: 'Editable Name' } as Dataset['editableProperties'],
            });
            expect(getSemanticModelDatasetDisplayName(dataset)).toBe('Editable Name');
        });

        it('prefers properties.name over name', () => {
            const dataset = buildDataset({
                name: 'raw.name',
                properties: { name: 'Logical Name' } as Dataset['properties'],
            });
            expect(getSemanticModelDatasetDisplayName(dataset)).toBe('Logical Name');
        });

        it('falls back to name then urn', () => {
            expect(getSemanticModelDatasetDisplayName(buildDataset({ name: 'raw.name' }))).toBe('raw.name');
            expect(getSemanticModelDatasetDisplayName(buildDataset({ name: undefined }))).toBe(
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)',
            );
        });
    });

    describe('getSemanticModelDatasetDescription', () => {
        it('prefers editable description over properties description', () => {
            const dataset = buildDataset({
                properties: { description: 'props desc' } as Dataset['properties'],
                editableProperties: { description: 'editable desc' } as Dataset['editableProperties'],
            });
            expect(getSemanticModelDatasetDescription(dataset)).toBe('editable desc');
        });

        it('returns undefined when no description exists', () => {
            expect(getSemanticModelDatasetDescription(buildDataset())).toBeUndefined();
        });
    });

    describe('getSemanticModelDatasetLabel', () => {
        it('prefers semantic model alias', () => {
            const dataset = buildDataset({
                semanticModelProperties: { alias: 'OPPORTUNITIES' } as Dataset['semanticModelProperties'],
                properties: { name: 'Logical Name' } as Dataset['properties'],
            });
            expect(getSemanticModelDatasetLabel(dataset)).toBe('OPPORTUNITIES');
        });

        it('falls back to display name without alias', () => {
            const dataset = buildDataset({
                properties: { name: 'Logical Name' } as Dataset['properties'],
            });
            expect(getSemanticModelDatasetLabel(dataset)).toBe('Logical Name');
        });
    });

    describe('withSemanticModelAlias', () => {
        it('returns the same dataset when alias is missing', () => {
            const dataset = buildDataset();
            expect(withSemanticModelAlias(dataset)).toBe(dataset);
        });

        it('overrides properties.name with alias and clears editable name', () => {
            const dataset = buildDataset({
                semanticModelProperties: { alias: 'ACCOUNTS' } as Dataset['semanticModelProperties'],
                properties: { name: 'Logical Name' } as Dataset['properties'],
                editableProperties: { name: 'Editable Name' } as Dataset['editableProperties'],
            });
            const aliased = withSemanticModelAlias(dataset);
            expect(aliased.properties?.name).toBe('ACCOUNTS');
            expect(aliased.editableProperties?.name).toBeUndefined();
            expect(dataset.properties?.name).toBe('Logical Name');
        });
        it('creates properties with alias when properties is missing', () => {
            const dataset = buildDataset({
                semanticModelProperties: { alias: 'ACCOUNTS' } as Dataset['semanticModelProperties'],
            });
            const aliased = withSemanticModelAlias(dataset);
            expect(aliased.properties?.name).toBe('ACCOUNTS');
        });
    });
});
