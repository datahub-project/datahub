import { renderHook } from '@testing-library/react-hooks';
import { Mock, vi } from 'vitest';

import useStructuredProp from '@app/govern/structuredProperties/useStructuredProp';
import { useEntityRegistry } from '@app/useEntityRegistry';

vi.mock('@app/govern/structuredProperties/utils', () => ({
    getEntityTypeUrn: vi.fn((_, entityType) => `urn:li:entityType:${entityType}`),
    valueTypes: [
        { value: 'typeSingle', label: 'Single', cardinality: 'SINGLE' },
        { value: 'typeMultiple', label: 'Multiple', cardinality: 'MULTIPLE' },
    ],
}));

vi.mock('@app/useEntityRegistry');

describe('useStructuredProp', () => {
    beforeEach(() => {
        (useEntityRegistry as Mock).mockReturnValue({
            getEntityName: vi.fn(),
        });
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should return correct initial values', () => {
        const { result } = renderHook(() =>
            useStructuredProp({
                form: {} as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        expect(result.current.disabledEntityTypeValues).toEqual(undefined);
        expect(result.current.disabledTypeQualifierValues).toEqual(undefined);
    });

    it('should return a list of entities', () => {
        const { result } = renderHook(() =>
            useStructuredProp({
                form: {} as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        const entities = result.current.getEntitiesListOptions(['DATASET' as any]);
        expect(entities.length).toEqual(1);
    });

    it('should update form values on select change', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleSelectChange('field', 'value');
        expect(setFormValues).toHaveBeenCalled();
    });

    it('should update form values on select update change', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
                selectedProperty: {
                    definition: {
                        entityTypes: [],
                        typeQualifier: {
                            allowedTypes: [],
                        },
                    },
                } as any,
            }),
        );

        result.current.handleSelectUpdateChange('field', []);
        expect(setFormValues).toHaveBeenCalled();
    });

    it('should update form values on type update', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleTypeUpdate('type');
        expect(setFormValues).toHaveBeenCalled();
    });

    it('should update form values on display setting change', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('field', true);
        expect(setFormValues).toHaveBeenCalled();
    });

    it('should reset all settings when isHidden is true', () => {
        const setFormValues = vi.fn();
        const setFieldValue = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('isHidden', true);
        expect(setFormValues).toHaveBeenCalledWith(expect.any(Function));
        expect(setFieldValue).toHaveBeenCalledTimes(7);
    });

    it('should disable hideInAssetSummaryWhenEmpty when showInAssetSummary is false', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('showInAssetSummary', false);
        expect(setFormValues).toHaveBeenCalledWith(expect.any(Function));
    });

    it('should return disabled entity type values', () => {
        const { result } = renderHook(() =>
            useStructuredProp({
                form: {} as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
                selectedProperty: {
                    definition: {
                        entityTypes: [{ urn: 'urn:li:entityType:DATASET' }],
                    },
                } as any,
            }),
        );

        expect(result.current.disabledEntityTypeValues).toEqual(['urn:li:entityType:DATASET']);
    });

    it('should return disabled type qualifier values', () => {
        const { result } = renderHook(() =>
            useStructuredProp({
                form: {} as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
                selectedProperty: {
                    definition: {
                        typeQualifier: {
                            allowedTypes: [{ urn: 'urn:li:entityType:DATASET' }],
                        },
                    },
                } as any,
            }),
        );

        expect(result.current.disabledTypeQualifierValues).toEqual(['urn:li:entityType:DATASET']);
    });

    it('should set cardinality to Multiple when type has multiple cardinality', () => {
        const setCardinality = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues: vi.fn(),
                setCardinality,
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleTypeUpdate('typeMultiple');
        expect(setCardinality).toHaveBeenCalledWith('MULTIPLE');
    });

    it('should set cardinality to Single when type has single cardinality', () => {
        const setCardinality = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues: vi.fn(),
                setCardinality,
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleTypeUpdate('typeSingle');
        expect(setCardinality).toHaveBeenCalledWith('SINGLE');
    });

    it('should handle typeQualifier in handleSelectChange', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleSelectChange('typeQualifier', ['value']);
        expect(setFormValues).toHaveBeenCalled();
    });

    it('should handle entityTypes in handleSelectUpdateChange', () => {
        const setFieldValue = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue } as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
                selectedProperty: {
                    definition: {
                        entityTypes: [{ urn: 'urn1' }],
                    },
                } as any,
            }),
        );

        result.current.handleSelectUpdateChange('entityTypes', ['urn2']);
        expect(setFieldValue).toHaveBeenCalledWith('entityTypes', ['urn1', 'urn2']);
    });

    it('should handle typeQualifier in handleSelectUpdateChange', () => {
        const setFieldValue = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue } as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
                selectedProperty: {
                    definition: {
                        typeQualifier: {
                            allowedTypes: [{ urn: 'urn1' }],
                        },
                    },
                } as any,
            }),
        );

        result.current.handleSelectUpdateChange('typeQualifier', ['urn2']);
        expect(setFieldValue).toHaveBeenCalledWith('typeQualifier', ['urn1', 'urn2']);
    });

    it('should not reset settings when isHidden is false', () => {
        const setFormValues = vi.fn();
        const setFieldValue = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('isHidden', false);
        expect(setFormValues).toHaveBeenCalledWith(expect.any(Function));
        expect(setFieldValue).toHaveBeenCalledWith(['settings', 'isHidden'], false);
        expect(setFieldValue).toHaveBeenCalledTimes(1);
    });

    it('should enable showInAssetSummary', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('showInAssetSummary', true);
        expect(setFormValues).toHaveBeenCalledWith(expect.any(Function));
    });

    it('should handle display setting change with previous settings', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        // Set an initial value
        result.current.handleDisplaySettingChange('showInSearchFilters', true);

        // Update another value
        result.current.handleDisplaySettingChange('showAsAssetBadge', true);

        expect(setFormValues).toHaveBeenCalledTimes(2);
    });

    it('should handle null entity name in getEntitiesListOptions', () => {
        (useEntityRegistry as Mock).mockReturnValue({
            getEntityName: vi.fn().mockReturnValue(null),
        });
        const { result } = renderHook(() =>
            useStructuredProp({
                form: {} as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        const entities = result.current.getEntitiesListOptions(['DATASET' as any]);
        expect(entities[0].label).toEqual('');
    });

    it('should correctly update form values using previous state', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleSelectChange('field', 'value');

        const updaterFn = setFormValues.mock.calls[0][0];
        const newState = updaterFn({ existing: 'data' });
        expect(newState).toEqual({ existing: 'data', field: 'value' });
    });

    it('should handle missing typeQualifier in handleSelectUpdateChange', () => {
        const setFieldValue = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue } as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
                selectedProperty: {
                    definition: {
                        entityTypes: [],
                    },
                } as any,
            }),
        );

        result.current.handleSelectUpdateChange('typeQualifier', ['urn2']);
        expect(setFieldValue).toHaveBeenCalledWith('typeQualifier', ['urn2']);
    });

    it('should default to single cardinality if type not found in handleTypeUpdate', () => {
        const setCardinality = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues: vi.fn(),
                setCardinality,
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleTypeUpdate('nonexistentType');
        expect(setCardinality).toHaveBeenCalledWith('SINGLE');
    });

    it('should handle display setting change when previous settings are undefined', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('showInSearchFilters', true);

        const updaterFn = setFormValues.mock.calls[0][0];
        let newState = updaterFn(undefined);
        expect(newState.settings.showInSearchFilters).toBe(true);

        newState = updaterFn({ some: 'value' });
        expect(newState.settings.showInSearchFilters).toBe(true);
    });

    it('should handle showInAssetSummary change when previous settings are undefined', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('showInAssetSummary', false);

        const updaterFn = setFormValues.mock.calls[0][0];
        const newState = updaterFn(undefined);
        expect(newState.settings.showInAssetSummary).toBe(false);
        expect(newState.settings.hideInAssetSummaryWhenEmpty).toBe(false);
    });

    it('should correctly update settings when isHidden is true', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('isHidden', true);

        const updaterFn = setFormValues.mock.calls[0][0];
        const newState = updaterFn({ existing: 'data' });
        expect(newState).toEqual({
            existing: 'data',
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

    it('should correctly update settings when showInAssetSummary is false', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('showInAssetSummary', false);

        const updaterFn = setFormValues.mock.calls[0][0];
        const prevState = { settings: { showInAssetSummary: true, hideInAssetSummaryWhenEmpty: true, other: 'value' } };
        const newState = updaterFn(prevState);
        expect(newState).toEqual({
            settings: {
                showInAssetSummary: false,
                hideInAssetSummaryWhenEmpty: false,
                other: 'value',
            },
        });
    });

    it('should correctly update settings for other fields', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('showAsAssetBadge', true);

        const updaterFn = setFormValues.mock.calls[0][0];
        const prevState = { settings: { showInSearchFilters: true } };
        const newState = updaterFn(prevState);
        expect(newState).toEqual({
            settings: {
                showInSearchFilters: true,
                showAsAssetBadge: true,
            },
        });
    });

    it('should return an empty array from getEntitiesListOptions when entitiesList is empty', () => {
        const { result } = renderHook(() =>
            useStructuredProp({
                form: {} as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        const entities = result.current.getEntitiesListOptions([]);
        expect(entities).toEqual([]);
    });

    it('should correctly map entity types in getEntitiesListOptions', () => {
        (useEntityRegistry as Mock).mockReturnValue({
            getEntityName: vi.fn().mockReturnValue('Dataset'),
        });
        const { result } = renderHook(() =>
            useStructuredProp({
                form: {} as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        const entities = result.current.getEntitiesListOptions(['DATASET' as any]);
        expect(entities).toEqual([{ label: 'Dataset', value: 'urn:li:entityType:DATASET' }]);
    });

    it('should update form values correctly for other fields in updateFormValues', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleSelectChange('someField', 'someValue');
        const updaterFn = setFormValues.mock.calls[0][0];
        const newState = updaterFn({ existing: 'data' });
        expect(newState).toEqual({ existing: 'data', someField: 'someValue' });
    });

    it('should handle empty initial values in handleSelectUpdateChange', () => {
        const setFieldValue = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue } as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
                selectedProperty: {
                    definition: {
                        entityTypes: [],
                        typeQualifier: { allowedTypes: [] },
                    },
                } as any,
            }),
        );

        result.current.handleSelectUpdateChange('entityTypes', ['urn1']);
        expect(setFieldValue).toHaveBeenCalledWith('entityTypes', ['urn1']);

        result.current.handleSelectUpdateChange('typeQualifier', ['urn2']);
        expect(setFieldValue).toHaveBeenCalledWith('typeQualifier', ['urn2']);
    });

    it('should merge values without duplicates in handleSelectUpdateChange', () => {
        const setFieldValue = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue } as any,
                setFormValues: vi.fn(),
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
                selectedProperty: {
                    entity: {
                        definition: {
                            entityTypes: [{ urn: 'urn1' }],
                        },
                    },
                } as any,
            }),
        );

        result.current.handleSelectUpdateChange('entityTypes', ['urn1', 'urn2']);
        expect(setFieldValue).toHaveBeenCalledWith('entityTypes', ['urn1', 'urn2']);
    });

    it('should call setSelectedValueType and handleSelectChange in handleTypeUpdate', () => {
        const setSelectedValueType = vi.fn();
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType,
            }),
        );

        result.current.handleTypeUpdate('typeSingle');
        expect(setSelectedValueType).toHaveBeenCalledWith('typeSingle');
        const updaterFn = setFormValues.mock.calls[0][0];
        const newState = updaterFn({});
        expect(newState).toEqual({ valueType: 'typeSingle' });
    });

    it('should handle undefined previous settings in handleDisplaySettingChange', () => {
        const setFormValues = vi.fn();
        const { result } = renderHook(() =>
            useStructuredProp({
                form: { setFieldValue: vi.fn() } as any,
                setFormValues,
                setCardinality: vi.fn(),
                setSelectedValueType: vi.fn(),
            }),
        );

        result.current.handleDisplaySettingChange('showInSearchFilters', true);
        const updaterFn = setFormValues.mock.calls[0][0];
        const newState = updaterFn(undefined);
        expect(newState.settings.showInSearchFilters).toBe(true);
    });
});
