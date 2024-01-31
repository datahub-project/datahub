import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import translateFieldPath from '../../../../../../dataset/profile/schema/utils/translateFieldPath';
import TypeLabel from '../TypeLabel';
import PrimaryKeyLabel from '../PrimaryKeyLabel';
import PartitioningKeyLabel from '../PartitioningKeyLabel';
import NullableLabel from '../NullableLabel';
import MenuColumn from '../MenuColumn';
import { ANTD_GRAY_V2 } from '../../../../../constants';
import { SchemaField } from '../../../../../../../../types.generated';

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
