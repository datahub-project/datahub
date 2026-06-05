import { describe, expect, it } from 'vitest';

import { viewBuilderProperties } from '@app/entityV2/view/builder/viewBuilderProperties';
import { SelectInputMode, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';

import { EntityType } from '@types';

describe('viewBuilderProperties', () => {
    it('should export an array of properties', () => {
        expect(Array.isArray(viewBuilderProperties)).toBe(true);
        expect(viewBuilderProperties.length).toBeGreaterThan(0);
    });

    it('should have unique property IDs', () => {
        const ids = viewBuilderProperties.map((p) => p.id);
        const uniqueIds = new Set(ids);
        expect(uniqueIds.size).toBe(ids.length);
    });

    it('should include _entityType property with correct display name and ENUM options', () => {
        const entityTypeProp = viewBuilderProperties.find((p) => p.id === '_entityType');
        expect(entityTypeProp?.displayName).toBe('Type');
        expect(entityTypeProp?.description).toBe('The type of the asset.');
        expect(entityTypeProp?.valueType).toBe(ValueTypeId.ENUM);
        expect(entityTypeProp?.valueOptions?.mode).toBe(SelectInputMode.MULTIPLE);
        expect(Array.isArray(entityTypeProp?.valueOptions?.options)).toBe(true);
        expect(entityTypeProp?.valueOptions?.options.length).toBeGreaterThan(0);
    });

    it('should include typeNames property with correct display name and aggregation', () => {
        const typeNamesProp = viewBuilderProperties.find((p) => p.id === 'typeNames');
        expect(typeNamesProp?.displayName).toBe('Sub Type');
        expect(typeNamesProp?.valueType).toBe(ValueTypeId.ENUM);
        expect(typeNamesProp?.valueOptions?.aggregationField).toBe('typeNames');
    });

    it('should include platform property with correct display name', () => {
        const platformProp = viewBuilderProperties.find((p) => p.id === 'platform');
        expect(platformProp?.displayName).toBe('Platform');
        expect(platformProp?.description).toBe('The data platform where the asset lives.');
        expect(platformProp?.valueType).toBe(ValueTypeId.URN);
        expect(platformProp?.valueOptions?.entityTypes).toContain(EntityType.DataPlatform);
    });

    it('should include owners property with correct display name', () => {
        const ownersProp = viewBuilderProperties.find((p) => p.id === 'owners');
        expect(ownersProp?.displayName).toBe('Owner');
        expect(ownersProp?.description).toBe('The owners of an asset.');
        expect(ownersProp?.valueType).toBe(ValueTypeId.URN);
        expect(ownersProp?.valueOptions?.entityTypes).toContain(EntityType.CorpUser);
        expect(ownersProp?.valueOptions?.entityTypes).toContain(EntityType.CorpGroup);
    });

    it('should include domains property with correct display name', () => {
        const domainsProp = viewBuilderProperties.find((p) => p.id === 'domains');
        expect(domainsProp?.displayName).toBe('Domain');
        expect(domainsProp?.valueOptions?.entityTypes).toContain(EntityType.Domain);
    });

    it('should include dataProducts property with correct display name', () => {
        const dataProductsProp = viewBuilderProperties.find((p) => p.id === 'dataProducts');
        expect(dataProductsProp?.displayName).toBe('Data Product');
        expect(dataProductsProp?.description).toBe('The data product the asset belongs to.');
        expect(dataProductsProp?.valueOptions?.entityTypes).toContain(EntityType.DataProduct);
    });

    it('should include tags and glossaryTerms properties with correct display names', () => {
        const tagsProp = viewBuilderProperties.find((p) => p.id === 'tags');
        expect(tagsProp?.displayName).toBe('Tags');
        expect(tagsProp?.valueOptions?.entityTypes).toContain(EntityType.Tag);

        const glossaryProp = viewBuilderProperties.find((p) => p.id === 'glossaryTerms');
        expect(glossaryProp?.displayName).toBe('Glossary Terms');
        expect(glossaryProp?.valueOptions?.entityTypes).toContain(EntityType.GlossaryTerm);
    });

    it('should include container property with correct display name', () => {
        const containerProp = viewBuilderProperties.find((p) => p.id === 'container');
        expect(containerProp?.displayName).toBe('Container');
        expect(containerProp?.description).toBe('The parent container of the asset.');
        expect(containerProp?.valueOptions?.entityTypes).toContain(EntityType.Container);
    });

    it('should include column field properties with correct display names', () => {
        const fieldPathsProp = viewBuilderProperties.find((p) => p.id === 'fieldPaths');
        expect(fieldPathsProp?.displayName).toBe('Column Name');
        expect(fieldPathsProp?.description).toBe('The name of a schema field / column.');
        expect(fieldPathsProp?.valueType).toBe(ValueTypeId.STRING);

        const fieldTagsProp = viewBuilderProperties.find((p) => p.id === 'fieldTags');
        expect(fieldTagsProp?.displayName).toBe('Column Tag');
        expect(fieldTagsProp?.valueOptions?.entityTypes).toContain(EntityType.Tag);

        const fieldGlossaryProp = viewBuilderProperties.find((p) => p.id === 'fieldGlossaryTerms');
        expect(fieldGlossaryProp?.displayName).toBe('Column Glossary Term');
        expect(fieldGlossaryProp?.valueOptions?.entityTypes).toContain(EntityType.GlossaryTerm);
    });

    it('should include boolean properties with correct English display names', () => {
        expect(viewBuilderProperties.find((p) => p.id === 'hasDescription')?.displayName).toBe('Has Description');
        expect(viewBuilderProperties.find((p) => p.id === 'removed')?.displayName).toBe('Soft Deleted');
        expect(viewBuilderProperties.find((p) => p.id === 'hasActiveIncidents')?.displayName).toBe(
            'Has Active Incidents',
        );
        expect(viewBuilderProperties.find((p) => p.id === 'hasFailingAssertions')?.displayName).toBe(
            'Has Failing Assertions',
        );

        const booleanIds = ['hasDescription', 'removed', 'hasActiveIncidents', 'hasFailingAssertions'];
        booleanIds.forEach((id) => {
            expect(viewBuilderProperties.find((p) => p.id === id)?.valueType).toBe(ValueTypeId.BOOLEAN);
        });
    });

    it('should include origin property with correct display name and environment options', () => {
        const originProp = viewBuilderProperties.find((p) => p.id === 'origin');
        expect(originProp?.displayName).toBe('Environment');
        expect(originProp?.description).toBe('The environment / origin of the asset (e.g. PROD, DEV).');
        expect(originProp?.valueType).toBe(ValueTypeId.ENUM);
        const options = originProp?.valueOptions?.options ?? [];
        expect(options.find((o: { id: string }) => o.id === 'PROD')?.displayName).toBe('Production');
        expect(options.find((o: { id: string }) => o.id === 'DEV')?.displayName).toBe('Development');
        expect(options.find((o: { id: string }) => o.id === 'STAGING')?.displayName).toBe('Staging');
    });

    it('should include platformInstance property with correct display name and aggregation', () => {
        const platformInstanceProp = viewBuilderProperties.find((p) => p.id === 'platformInstance');
        expect(platformInstanceProp?.displayName).toBe('Platform Instance');
        expect(platformInstanceProp?.description).toBe('The specific platform instance where the asset lives.');
        expect(platformInstanceProp?.valueOptions?.aggregationField).toBe('platformInstance');
    });
});
