import { renderHook } from '@testing-library/react-hooks';
import { Mock, vi } from 'vitest';

import useStructuredProp from '@app/govern/structuredProperties/useStructuredProp';
import { StructuredProp } from '@app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityType, StructuredPropertyEntity } from '@src/types.generated';

vi.mock('@app/govern/structuredProperties/utils', () => ({
    getEntityTypeUrn: vi.fn((_, entityType) => `urn:li:entityType:${entityType}`),
    valueTypes: [
        { value: 'typeSingle', label: 'Single', cardinality: 'SINGLE' },
        { value: 'typeMultiple', label: 'Multiple', cardinality: 'MULTIPLE' },
    ],
}));

vi.mock('@app/useEntityRegistry');

type SetupOptions = {
    selectedProperty?: unknown;
};

function setup({ selectedProperty }: SetupOptions = {}) {
    const setFormValues = vi.fn();
    const setCardinality = vi.fn();
    const setSelectedValueType = vi.fn();

    const { result } = renderHook(() =>
        useStructuredProp({
            selectedProperty: selectedProperty as StructuredPropertyEntity | undefined,
            setFormValues,
            setCardinality,
            setSelectedValueType,
        }),
    );

    // Every handler updates state through an updater function; applying it to a previous state is
    // how we observe what the handler actually does.
    const applyUpdate = (previousValues: StructuredProp = {}) => setFormValues.mock.calls[0][0](previousValues);

    return { result, setFormValues, setCardinality, setSelectedValueType, applyUpdate };
}

describe('useStructuredProp', () => {
    beforeEach(() => {
        (useEntityRegistry as Mock).mockReturnValue({ getEntityName: vi.fn().mockReturnValue('Dataset') });
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    describe('getEntitiesListOptions', () => {
        it('maps entity types to label/value options', () => {
            const { result } = setup();

            expect(result.current.getEntitiesListOptions([EntityType.Dataset])).toEqual([
                { label: 'Dataset', value: 'urn:li:entityType:DATASET' },
            ]);
        });

        it('falls back to an empty label when the entity has no name', () => {
            (useEntityRegistry as Mock).mockReturnValue({ getEntityName: vi.fn().mockReturnValue(null) });
            const { result } = setup();

            expect(result.current.getEntitiesListOptions([EntityType.Dataset])?.[0].label).toEqual('');
        });
    });

    describe('handleSelectChange', () => {
        it('sets the selected values on the named field', () => {
            const { result, applyUpdate } = setup();

            result.current.handleSelectChange('entityTypes', ['urn1']);

            expect(applyUpdate({ displayName: 'existing' })).toEqual({
                displayName: 'existing',
                entityTypes: ['urn1'],
            });
        });

        it('nests allowed types under typeQualifier', () => {
            const { result, applyUpdate } = setup();

            result.current.handleSelectChange(['typeQualifier', 'allowedTypes'], ['urn1']);

            expect(applyUpdate()).toEqual({ typeQualifier: { allowedTypes: ['urn1'] } });
        });
    });

    describe('handleSelectUpdateChange', () => {
        it('keeps values already saved on the property', () => {
            const { result, applyUpdate } = setup({
                selectedProperty: { definition: { entityTypes: [{ urn: 'urn1' }] } },
            });

            result.current.handleSelectUpdateChange('entityTypes', ['urn2']);

            expect(applyUpdate()).toEqual({ entityTypes: ['urn1', 'urn2'] });
        });

        it('does not duplicate a value that is already saved', () => {
            const { result, applyUpdate } = setup({
                selectedProperty: { definition: { entityTypes: [{ urn: 'urn1' }] } },
            });

            result.current.handleSelectUpdateChange('entityTypes', ['urn1', 'urn2']);

            expect(applyUpdate()).toEqual({ entityTypes: ['urn1', 'urn2'] });
        });

        it('keeps saved allowed platforms', () => {
            const { result, applyUpdate } = setup({
                selectedProperty: { definition: { allowedPlatforms: [{ urn: 'urn:li:dataPlatform:snowflake' }] } },
            });

            result.current.handleSelectUpdateChange('allowedPlatforms', ['urn:li:dataPlatform:bigquery']);

            expect(applyUpdate()).toEqual({
                allowedPlatforms: ['urn:li:dataPlatform:snowflake', 'urn:li:dataPlatform:bigquery'],
            });
        });

        it('handles a property with nothing saved yet', () => {
            const { result, applyUpdate } = setup({ selectedProperty: { definition: {} } });

            result.current.handleSelectUpdateChange(['typeQualifier', 'allowedTypes'], ['urn2']);

            expect(applyUpdate()).toEqual({ typeQualifier: { allowedTypes: ['urn2'] } });
        });
    });

    describe('handleTypeUpdate', () => {
        it('records the selected type and its cardinality', () => {
            const { result, setCardinality, setSelectedValueType, applyUpdate } = setup();

            result.current.handleTypeUpdate('typeMultiple');

            expect(setSelectedValueType).toHaveBeenCalledWith('typeMultiple');
            expect(setCardinality).toHaveBeenCalledWith('MULTIPLE');
            expect(applyUpdate()).toEqual({ valueType: 'typeMultiple' });
        });

        it('defaults to single cardinality for an unknown type', () => {
            const { result, setCardinality } = setup();

            result.current.handleTypeUpdate('nonexistentType');

            expect(setCardinality).toHaveBeenCalledWith('SINGLE');
        });
    });

    describe('handleDisplaySettingChange', () => {
        it('turns off every other display surface when the property is hidden', () => {
            const { result, applyUpdate } = setup();

            result.current.handleDisplaySettingChange('isHidden', true);

            expect(applyUpdate({ displayName: 'existing' })).toEqual({
                displayName: 'existing',
                settings: {
                    isHidden: true,
                    showInSearchFilters: false,
                    showAsAssetBadge: false,
                    showInAssetSummary: false,
                    hideInAssetSummaryWhenEmpty: false,
                    showInColumnsTable: false,
                },
            });
        });

        it('clears hideInAssetSummaryWhenEmpty when the asset sidebar is turned off', () => {
            const { result, applyUpdate } = setup();

            result.current.handleDisplaySettingChange('showInAssetSummary', false);

            expect(
                applyUpdate({
                    settings: {
                        isHidden: false,
                        showInSearchFilters: false,
                        showAsAssetBadge: false,
                        showInAssetSummary: true,
                        hideInAssetSummaryWhenEmpty: true,
                        showInColumnsTable: false,
                    },
                }),
            ).toEqual({
                settings: {
                    isHidden: false,
                    showInSearchFilters: false,
                    showAsAssetBadge: false,
                    showInAssetSummary: false,
                    hideInAssetSummaryWhenEmpty: false,
                    showInColumnsTable: false,
                },
            });
        });

        it('leaves the other settings untouched when toggling one of them', () => {
            const { result, applyUpdate } = setup();

            result.current.handleDisplaySettingChange('showAsAssetBadge', true);

            expect(
                applyUpdate({
                    settings: {
                        isHidden: false,
                        showInSearchFilters: true,
                        showAsAssetBadge: false,
                        showInAssetSummary: false,
                        hideInAssetSummaryWhenEmpty: false,
                        showInColumnsTable: false,
                    },
                }),
            ).toEqual({
                settings: {
                    isHidden: false,
                    showInSearchFilters: true,
                    showAsAssetBadge: true,
                    showInAssetSummary: false,
                    hideInAssetSummaryWhenEmpty: false,
                    showInColumnsTable: false,
                },
            });
        });

        it('works when no settings have been set yet', () => {
            const { result, applyUpdate } = setup();

            result.current.handleDisplaySettingChange('showInSearchFilters', true);

            expect(applyUpdate(undefined).settings.showInSearchFilters).toBe(true);
        });
    });

    describe('disabled values', () => {
        it('returns the entity types already saved on the property', () => {
            const { result } = setup({
                selectedProperty: { definition: { entityTypes: [{ urn: 'urn:li:entityType:DATASET' }] } },
            });

            expect(result.current.disabledEntityTypeValues).toEqual(['urn:li:entityType:DATASET']);
        });

        it('returns the allowed types already saved on the property', () => {
            const { result } = setup({
                selectedProperty: {
                    definition: { typeQualifier: { allowedTypes: [{ urn: 'urn:li:entityType:DATASET' }] } },
                },
            });

            expect(result.current.disabledTypeQualifierValues).toEqual(['urn:li:entityType:DATASET']);
        });

        it('returns undefined when nothing is saved on the property', () => {
            const { result } = setup();

            expect(result.current.disabledEntityTypeValues).toEqual(undefined);
            expect(result.current.disabledTypeQualifierValues).toEqual(undefined);
        });
    });
});
