import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import translateFieldPath from '@app/entity/dataset/profile/schema/utils/translateFieldPath';
import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import MenuColumn from '@app/entity/shared/tabs/Dataset/Schema/components/MenuColumn';
import NullableLabel from '@app/entity/shared/tabs/Dataset/Schema/components/NullableLabel';
import PartitioningKeyLabel from '@app/entity/shared/tabs/Dataset/Schema/components/PartitioningKeyLabel';
import PrimaryKeyLabel from '@app/entity/shared/tabs/Dataset/Schema/components/PrimaryKeyLabel';
import TypeLabel from '@app/entity/shared/tabs/Dataset/Schema/components/TypeLabel';

import { SchemaField } from '@types';

const FieldHeaderWrapper = styled.div`
    padding: 16px;
    display: flex;
    justify-content: space-between;
    border-bottom: 1px solid ${ANTD_GRAY_V2[4]};
`;

const FieldName = styled(Typography.Text)`
    font-size: 16px;
    font-family: 'Roboto Mono', monospace;
`;

const TypesSection = styled.div`
    margin-left: -4px;
    margin-top: 8px;
`;

const NameTypesWrapper = styled.div`
    overflow: hidden;
`;

const MenuWrapper = styled.div`
    margin-right: 5px;
`;

interface Props {
    expandedField: SchemaField;
}

export default function FieldHeader({ expandedField }: Props) {
    const displayName = translateFieldPath(expandedField.fieldPath || '');
    return (
        <FieldHeaderWrapper>
            <NameTypesWrapper>
                <FieldName>{displayName}</FieldName>
                <TypesSection>
                    <TypeLabel type={expandedField.type} nativeDataType={expandedField.nativeDataType} />
                    {expandedField.isPartOfKey && <PrimaryKeyLabel />}
                    {expandedField.isPartitioningKey && <PartitioningKeyLabel />}
                    {expandedField.nullable && <NullableLabel />}
                </TypesSection>
            </NameTypesWrapper>
            <MenuWrapper>
                <MenuColumn field={expandedField} />
            </MenuWrapper>
        </FieldHeaderWrapper>
    );
}
