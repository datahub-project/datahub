/**
 * A link to a column (`/Columns?highlightedPath=<field>`) starts the Columns tab with that column's drawer
 * open. SchemaTable closes the drawer when the page path changes (moving to another tab), but must not do
 * so when it first mounts, or every link to a column lands with nothing selected.
 */
import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { useHistory } from 'react-router';
import { vi } from 'vitest';

import SchemaTable from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaTable';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,petshop.public.pet_orders,PROD)';
const COLUMNS_PATH = `/dataset/${encodeURIComponent(DATASET_URN)}/Columns`;

vi.mock('@app/entityV2/shared/EntityContext', async (importOriginal) => ({
    ...(await importOriginal<any>()),
    useEntityData: () => ({ urn: DATASET_URN, entityType: 'DATASET', entityData: null }),
    useMutationUrn: () => DATASET_URN,
    useRefetch: () => vi.fn(),
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/utils/useGetTableColumnProperties', () => ({
    useGetTableColumnProperties: () => undefined,
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/SchemaFieldDrawer', () => ({
    default: () => null,
}));

// jsdom has no layout; render every row instead of a virtualised viewport.
vi.mock('virtualizedtableforantd4', () => ({
    useVT: () => [{}, () => {}, { current: null }],
}));

const fields = [
    { fieldPath: 'pet_name', type: 'STRING', nativeDataType: 'varchar', nullable: false, recursive: false },
] as any[];

let navigate: (path: string) => void = () => {};
function CaptureHistory() {
    const history = useHistory();
    navigate = (path) => history.push(path);
    return null;
}

function renderAt(url: string, setExpandedDrawerFieldPath: (path: string | null) => void) {
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer initialEntries={[url]}>
                <CaptureHistory />
                <SchemaTable
                    rows={fields}
                    schemaMetadata={{ name: 'pet_orders', fields } as any}
                    editableSchemaMetadata={null}
                    expandedDrawerFieldPath="pet_name"
                    setExpandedDrawerFieldPath={setExpandedDrawerFieldPath}
                />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('SchemaTable drawer selection', () => {
    it('keeps the column a link selected when the table first mounts', async () => {
        const setExpandedDrawerFieldPath = vi.fn();
        renderAt(`${COLUMNS_PATH}?highlightedPath=pet_name`, setExpandedDrawerFieldPath);

        await waitFor(() => expect(screen.getByTestId('schema-field-pet_name')).toBeInTheDocument());
        expect(setExpandedDrawerFieldPath).not.toHaveBeenCalledWith(null);
    });

    it('closes the drawer when the page path changes, but not when only the query changes', async () => {
        const setExpandedDrawerFieldPath = vi.fn();
        renderAt(`${COLUMNS_PATH}?highlightedPath=pet_name`, setExpandedDrawerFieldPath);
        await waitFor(() => expect(screen.getByTestId('schema-field-pet_name')).toBeInTheDocument());

        navigate(`${COLUMNS_PATH}?highlightedPath=pet_name&schemaFilter=pet`);
        expect(setExpandedDrawerFieldPath).not.toHaveBeenCalledWith(null);

        navigate(`/dataset/${encodeURIComponent(DATASET_URN)}/Lineage`);
        await waitFor(() => expect(setExpandedDrawerFieldPath).toHaveBeenCalledWith(null));
    });
});
