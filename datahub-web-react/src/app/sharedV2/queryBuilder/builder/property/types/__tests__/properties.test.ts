import { describe, expect, it } from 'vitest';

import {
    OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID,
    STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID,
} from '@app/sharedV2/queryBuilder/builder/property/constants';
import { Property, entityProperties } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';

import { EntityType } from '@types';

const getProps = (type: EntityType): Property[] => entityProperties.find((e) => e.type === type)?.properties ?? [];

describe('entityProperties', () => {
    it('should define properties for all supported entity types', () => {
        const types = entityProperties.map((e) => e.type);
        expect(types).toContain(EntityType.Dataset);
        expect(types).toContain(EntityType.Dashboard);
        expect(types).toContain(EntityType.Chart);
        expect(types).toContain(EntityType.DataFlow);
        expect(types).toContain(EntityType.DataJob);
        expect(types).toContain(EntityType.Container);
    });

    it('should include common properties in every entity type', () => {
        const commonIds = [
            'urn',
            'entityType',
            'globalTags.tags.tag',
            'glossaryTerms.terms.urn',
            'domains.domains',
            'ownership.owners.owner',
            'deprecation.deprecated',
        ];

        entityProperties.forEach(({ type: _type, properties }) => {
            commonIds.forEach((id) => {
                const found = properties.some((p: Property) => p.id === id);
                expect(found).toBe(true);
            });
        });
    });

    describe('common property display names', () => {
        it('should have correct English display names for shared properties', () => {
            const props = getProps(EntityType.Dataset);
            expect(props.find((p: Property) => p.id === 'urn')?.displayName).toBe('Urn');
            expect(props.find((p: Property) => p.id === 'entityType')?.displayName).toBe('Type');
            expect(props.find((p: Property) => p.id === 'globalTags.tags.tag')?.displayName).toBe('Tags');
            expect(props.find((p: Property) => p.id === 'glossaryTerms.terms.urn')?.displayName).toBe('Glossary Terms');
            expect(props.find((p: Property) => p.id === 'domains.domains')?.displayName).toBe('Domain');
            expect(props.find((p: Property) => p.id === 'ownership.owners.owner')?.displayName).toBe('Owners');
            expect(props.find((p: Property) => p.id === 'deprecation.deprecated')?.displayName).toBe('Deprecated');
            expect(props.find((p: Property) => p.id === 'container.container')?.displayName).toBe('Container');
            expect(props.find((p: Property) => p.id === '__firstSynchronized')?.displayName).toBe('First Synchronized');
            expect(props.find((p: Property) => p.id === '__lastSynchronized')?.displayName).toBe('Last Synchronized');
            expect(props.find((p: Property) => p.id === '__lastObserved')?.displayName).toBe('Last Observed');
        });
    });

    describe('Dataset properties', () => {
        it('should include dataset-specific properties with correct display names', () => {
            const props = getProps(EntityType.Dataset);

            expect(props.find((p: Property) => p.id === 'schemaFields')?.displayName).toBe('Columns');
            expect(props.find((p: Property) => p.id === 'schemaFields.length')?.displayName).toBe('Number of Columns');
            expect(
                props.find((p: Property) => p.id === STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID)?.displayName,
            ).toBe('Structured Property');
            expect(props.find((p: Property) => p.id === OWNERSHIP_TYPE_REFERENCE_PLACEHOLDER_ID)?.displayName).toBe(
                'Ownership Type',
            );
        });

        it('should have correct valueType for schemaFields', () => {
            const props = getProps(EntityType.Dataset);
            const schemaFields = props.find((p: Property) => p.id === 'schemaFields');
            expect(schemaFields?.valueType).toBe(ValueTypeId.SCHEMA_FIELD_LIST);
            expect(schemaFields?.description).toBe('The set of columns / fields associated with the dataset.');
        });

        it('should have metrics children with correct English display names', () => {
            const props = getProps(EntityType.Dataset);
            const metrics = props.find((p: Property) => p.id === 'datasetMetrics');
            expect(metrics?.displayName).toBe('Metrics');
            const children = metrics?.children ?? [];
            expect(children.find((c: Property) => c.id === 'usageFeatures.usageCountLast30Days')?.displayName).toBe(
                'Query Count in Last 30 Days',
            );
            expect(children.find((c: Property) => c.id === 'storageFeatures.rowCount')?.displayName).toBe(
                'Row Count Total',
            );
            expect(children.find((c: Property) => c.id === 'storageFeatures.sizeInBytes')?.displayName).toBe(
                'Size In Bytes',
            );
        });

        it('should have assertions children with correct English display names', () => {
            const props = getProps(EntityType.Dataset);
            const assertions = props.find((p: Property) => p.id === 'assertions');
            expect(assertions?.displayName).toBe('Assertions');
            const children = assertions?.children ?? [];
            expect(
                children.find((c: Property) => c.id === 'assertionsSummary.passingAssertionDetails')?.displayName,
            ).toBe('Passing Assertions');
            expect(
                children.find((c: Property) => c.id === 'assertionsSummary.failingAssertionDetails')?.displayName,
            ).toBe('Failing Assertions');
        });

        it('should have incidents children with correct English display names', () => {
            const props = getProps(EntityType.Dataset);
            const incidents = props.find((p: Property) => p.id === 'incidents');
            expect(incidents?.displayName).toBe('Incidents');
            const children = incidents?.children ?? [];
            expect(children.find((c: Property) => c.id === 'incidentsSummary.activeIncidentDetails')?.displayName).toBe(
                'Active Incidents',
            );
            expect(
                children.find((c: Property) => c.id === 'incidentsSummary.resolvedIncidentDetails')?.displayName,
            ).toBe('Resolved Incidents');
        });

        it('should have description children with correct English display names', () => {
            const props = getProps(EntityType.Dataset);
            const desc = props.find((p: Property) => p.id === 'datasetDescription');
            expect(desc?.displayName).toBe('Description');
            const children = desc?.children ?? [];
            expect(children.find((c: Property) => c.id === 'datasetProperties.description')?.displayName).toBe(
                'Native Platform Description',
            );
            expect(children.find((c: Property) => c.id === 'editableDatasetProperties.description')?.displayName).toBe(
                'DataHub Description',
            );
        });
    });

    describe('Dashboard properties', () => {
        it('should have dashboard metrics with correct English display names', () => {
            const props = getProps(EntityType.Dashboard);
            const metrics = props.find((p: Property) => p.id === 'dashboardMetrics');
            expect(metrics?.displayName).toBe('Metrics');
            const children = metrics?.children ?? [];
            expect(children.find((c: Property) => c.id === 'usageFeatures.viewCountTotal')?.displayName).toBe(
                'Total View Count',
            );
            expect(children.find((c: Property) => c.id === 'usageFeatures.viewCountLast30Days')?.displayName).toBe(
                'View Count in Last 30 Days',
            );
        });
    });

    describe('Container properties', () => {
        it('should include subtype property with correct display name', () => {
            const props = getProps(EntityType.Container);
            expect(props.find((p: Property) => p.id === 'subTypes.typeNames')?.displayName).toBe('Subtypes');
        });
    });
});
