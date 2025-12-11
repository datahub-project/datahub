/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
