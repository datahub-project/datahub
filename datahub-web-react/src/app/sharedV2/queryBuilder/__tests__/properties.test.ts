import { describe, expect, it } from 'vitest';

import { SelectInputMode, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';
import { properties } from '@app/sharedV2/queryBuilder/properties';

import { EntityType } from '@types';

describe('queryBuilder properties', () => {
    it('should export an array of properties', () => {
        expect(Array.isArray(properties)).toBe(true);
        expect(properties.length).toBeGreaterThan(0);
    });

    it('should have unique property IDs', () => {
        const ids = properties.map((p) => p.id);
        const uniqueIds = new Set(ids);
        expect(uniqueIds.size).toBe(ids.length);
    });

    it('should include _entityType property with correct display name and enum options', () => {
        const entityTypeProp = properties.find((p) => p.id === '_entityType');
        expect(entityTypeProp?.displayName).toBe('Type');
        expect(entityTypeProp?.description).toBe('The type of the asset.');
        expect(entityTypeProp?.valueType).toBe(ValueTypeId.ENUM);
        expect(entityTypeProp?.valueOptions?.mode).toBe(SelectInputMode.MULTIPLE);
        const optionIds = entityTypeProp?.valueOptions?.options?.map((o: { id: string }) => o.id);
        expect(optionIds).toContain('dataset');
        expect(optionIds).toContain('dashboard');
        expect(optionIds).toContain('chart');
        expect(optionIds).toContain('dataJob');
        expect(optionIds).toContain('dataFlow');
        expect(optionIds).toContain('container');
        expect(optionIds).toContain('domain');
        expect(optionIds).toContain('dataProduct');
        expect(optionIds).toContain('glossaryTerm');
    });

    it('should include platform property with correct display name', () => {
        const platformProp = properties.find((p) => p.id === 'platform');
        expect(platformProp?.displayName).toBe('Platform');
        expect(platformProp?.description).toBe('The data platform where the asset lives.');
        expect(platformProp?.valueType).toBe(ValueTypeId.URN);
        expect(platformProp?.valueOptions?.entityTypes).toContain(EntityType.DataPlatform);
    });

    it('should include container property with correct display name', () => {
        const containerProp = properties.find((p) => p.id === 'container');
        expect(containerProp?.displayName).toBe('Container');
        expect(containerProp?.description).toBe('The parent container of the asset.');
        expect(containerProp?.valueType).toBe(ValueTypeId.URN);
        expect(containerProp?.valueOptions?.entityTypes).toContain(EntityType.Container);
    });

    it('should include domains property with correct display name', () => {
        const domainsProp = properties.find((p) => p.id === 'domains');
        expect(domainsProp?.displayName).toBe('Domain');
        expect(domainsProp?.valueType).toBe(ValueTypeId.URN);
        expect(domainsProp?.valueOptions?.entityTypes).toContain(EntityType.Domain);
    });

    it('should include glossaryTerms property with correct display name', () => {
        const glossaryProp = properties.find((p) => p.id === 'glossaryTerms');
        expect(glossaryProp?.displayName).toBe('Glossary Terms');
        expect(glossaryProp?.valueType).toBe(ValueTypeId.URN);
        expect(glossaryProp?.valueOptions?.entityTypes).toContain(EntityType.GlossaryTerm);
    });

    it('should include tags property with correct display name', () => {
        const tagsProp = properties.find((p) => p.id === 'tags');
        expect(tagsProp?.displayName).toBe('Tags');
        expect(tagsProp?.valueType).toBe(ValueTypeId.URN);
        expect(tagsProp?.valueOptions?.entityTypes).toContain(EntityType.Tag);
    });

    it('should include owners property with correct display name', () => {
        const ownersProp = properties.find((p) => p.id === 'owners');
        expect(ownersProp?.displayName).toBe('Owned By');
        expect(ownersProp?.description).toBe('The owners of an asset');
        expect(ownersProp?.valueOptions?.entityTypes).toContain(EntityType.CorpUser);
        expect(ownersProp?.valueOptions?.entityTypes).toContain(EntityType.CorpGroup);
    });

    it('should include urn property with correct display name and many entity types', () => {
        const urnProp = properties.find((p) => p.id === 'urn');
        expect(urnProp?.displayName).toBe('Asset');
        expect(urnProp?.description).toBe('The specific asset itself');
        expect(urnProp?.valueType).toBe(ValueTypeId.URN);
        expect(urnProp?.valueOptions?.entityTypes).toContain(EntityType.Dataset);
        expect(urnProp?.valueOptions?.entityTypes).toContain(EntityType.Dashboard);
        expect(urnProp?.valueOptions?.entityTypes).toContain(EntityType.Chart);
    });
});
