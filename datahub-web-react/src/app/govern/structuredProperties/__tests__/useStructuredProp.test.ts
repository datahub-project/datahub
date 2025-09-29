import { renderHook } from '@testing-library/react-hooks';
import { Mock, vi } from 'vitest';

import useStructuredProp from '@app/govern/structuredProperties/useStructuredProp';
import { useEntityRegistry } from '@src/app/useEntityRegistry';

vi.mock('@app/govern/structuredProperties/utils', () => ({
    getEntityTypeUrn: vi.fn(),
    valueTypes: [],
}));

vi.mock('@src/app/useEntityRegistry');

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
                    entity: {
                        definition: {
                            entityTypes: [],
                            typeQualifier: {
                                allowedTypes: [],
                            },
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
});
