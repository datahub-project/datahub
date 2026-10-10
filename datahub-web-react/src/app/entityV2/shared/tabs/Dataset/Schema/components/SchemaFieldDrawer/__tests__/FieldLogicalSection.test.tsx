import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import FieldLogicalSection from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldLogicalSection';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType, SchemaField } from '@types';

const modalPropsMock = vi.fn();
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({
        config: { featureFlags: { logicalModelsEnabled: true } },
    }),
}));
vi.mock('@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal', () => ({
    EmbeddedListSearchModal: (props: unknown) => {
        modalPropsMock(props);
        return null;
    },
}));

const FIELD_URN = 'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,petshop.pet_orders,PROD),order_id)';
const CHILD_URN =
    'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,petshop_store_01.pet_orders,PROD),order_id)';
vi.mock('@graphql/schemaField.generated', () => ({
    useGetSchemaFieldQuery: () => ({
        data: {
            entity: {
                __typename: 'SchemaFieldEntity',
                urn: FIELD_URN,
                physicalChildren: {
                    total: 12,
                    relationships: [{ entity: { urn: CHILD_URN, type: EntityType.SchemaField } }],
                },
            },
        },
    }),
}));

describe('FieldLogicalSection', () => {
    it('applies the search bar view to "View all"', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <FieldLogicalSection
                        expandedField={{ fieldPath: 'order_id', schemaFieldEntity: { urn: FIELD_URN } } as SchemaField}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        fireEvent.click(screen.getByText(/11 more/));

        expect(modalPropsMock).toHaveBeenLastCalledWith(expect.objectContaining({ applyView: true }));
    });
});
