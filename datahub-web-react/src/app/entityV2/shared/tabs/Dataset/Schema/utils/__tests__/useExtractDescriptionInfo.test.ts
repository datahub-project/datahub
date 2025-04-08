import { renderHook } from '@testing-library/react-hooks';

import useExtractFieldDescriptionInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldDescriptionInfo';
import { EditableSchemaMetadata, SchemaField, SchemaFieldDataType } from '@src/types.generated';

describe('useExtractDescriptionInfo', () => {
    const emptyEditableSchemaMetadata: EditableSchemaMetadata = { editableSchemaFieldInfo: [] };

    const emptySchemaField: SchemaField = {
        fieldPath: 'testField',
        nullable: true,
        recursive: false,
        type: SchemaFieldDataType.String,
    };

    const { mockedGetFieldDescriptionDetails } = vi.hoisted(() => {
        return { mockedGetFieldDescriptionDetails: vi.fn() };
    });

    vi.mock('../getFieldDescriptionDetails', async (importOriginal) => {
        const original = await importOriginal<object>();
        return {
            ...original,
            getFieldDescriptionDetails: vi.fn(() => mockedGetFieldDescriptionDetails()),
        };
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('should extract description when it was provided', () => {
        mockedGetFieldDescriptionDetails.mockReturnValue({
            displayedDescription: 'testDescription',
            isPropagated: false,
            sourceDetail: '',
        });

        const extractFieldDescriptionInfo = renderHook(() =>
            useExtractFieldDescriptionInfo(emptyEditableSchemaMetadata),
        ).result.current;

        const { sanitizedDescription } = extractFieldDescriptionInfo(emptySchemaField);
        console.log(sanitizedDescription);
        expect(sanitizedDescription === 'testDescription').toBeTruthy();
    });
});
