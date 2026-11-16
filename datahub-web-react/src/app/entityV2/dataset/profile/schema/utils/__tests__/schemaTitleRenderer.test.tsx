import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import useSchemaTitleRenderer from '@app/entityV2/dataset/profile/schema/utils/schemaTitleRenderer';
import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { SchemaFieldDataType, SchemaMetadata } from '@types';

const schemaField = (fieldPath: string) =>
    ({
        fieldPath,
        nullable: false,
        recursive: false,
        type: SchemaFieldDataType.String,
        nativeDataType: 'string',
    }) as ExtendedSchemaFields;

const schemaMetadata = {
    primaryKeys: [],
    fields: [schemaField('id'), schemaField('col_a')],
} as unknown as SchemaMetadata;

function Title({ fieldPath, editedPrimaryKeys }: { fieldPath: string; editedPrimaryKeys: string[] }) {
    const renderTitle = useSchemaTitleRenderer('urn:li:dataset:1', schemaMetadata, editedPrimaryKeys, '');
    return renderTitle(fieldPath, schemaField(fieldPath));
}

describe('useSchemaTitleRenderer', () => {
    it('badges a field in the edited primary key when the source declares none', () => {
        const { getByText } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <Title fieldPath="id" editedPrimaryKeys={['id']} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Primary Key')).toBeInTheDocument();
    });

    it('does not badge fields outside the edited primary key', () => {
        const { queryByText } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <Title fieldPath="col_a" editedPrimaryKeys={['id']} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(queryByText('Primary Key')).not.toBeInTheDocument();
    });
});
