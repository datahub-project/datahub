import { CloseOutlined } from '@ant-design/icons';
import { useAppConfig } from '@app/useAppConfig';
import { Divider } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { SchemaField } from '../../../../../../../../types.generated';
import translateFieldPath from '../../../../../../dataset/profile/schema/utils/translateFieldPath';
import NullableLabel, { PartitioningKeyLabel, PrimaryKeyLabel } from '../ConstraintLabels';
import MenuColumn from '../MenuColumn';
import TypeLabel from '../TypeLabel';
import FieldPath from './FieldPath';
import { useEntityRegistry } from '../../../../../../../useEntityRegistry';

const FieldHeaderWrapper = styled.div`
    padding: 16px;
    display: flex;
    justify-content: space-between;
    border-bottom: 1px solid rgb(213, 213, 213);
`;

const NameTypesWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const TitleWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 16px;
`;

const RightGroup = styled.div`
    display: flex;
    align-items: baseline;
`;

const MenuWrapper = styled.div`
    margin-right: 5px;
`;

const FieldText = styled.div`
    font-size: 12px;
    line-height: 24px;
    color: #8d95b1;
`;

const CloseIcon = styled.div`
    font-size: 12px;
    padding: 0;
    height: 24px;
    width: 24px;
    display: flex;
    align-items: center;
    justify-content: center;

    &&:hover {
        cursor: pointer;
        stroke-width: 10px;
    }

    svg {
        height: 16px;
        width: 16px;
    }
`;

const StyledDivider = styled(Divider)`
    height: 75%;
    margin: 0;
`;

const StyledTypeLabel = styled(TypeLabel)`
    font-size: 14px;
    color: ${colors.gray[500]};
`;

const StyleLink = styled(Link)`
    color: ${colors.gray[800]};
    font-weight: 700;

    &:hover {
        text-decoration: underline;
    }
`;

interface Props {
    expandedField: SchemaField;
    setExpandedDrawerFieldPath: (fieldPath: string | null) => void;
}

export default function FieldHeader({ expandedField, setExpandedDrawerFieldPath }: Props) {
    const { config } = useAppConfig();
    const displayName = translateFieldPath(expandedField.fieldPath || '');
    const entityRegistry = useEntityRegistry();

    const linkEnabled = config.featureFlags.schemaFieldCLLEnabled;

    return (
        <FieldHeaderWrapper>
            <NameTypesWrapper>
                <TitleWrapper>
                    {linkEnabled ? (
                        <StyleLink
                            to={
                                expandedField.schemaFieldEntity &&
                                `${entityRegistry.getEntityUrl(
                                    expandedField.schemaFieldEntity?.type,
                                    (expandedField.schemaFieldEntity?.urn as string) || '',
                                )}/Lineage`
                            }
                        >
                            {displayName.split('.').pop()}
                        </StyleLink>
                    ) : (
                        displayName.split('.').pop()
                    )}
                    <StyledDivider type="vertical" />
                    <StyledTypeLabel type={expandedField.type} nativeDataType={expandedField.nativeDataType} />
                    {expandedField.isPartOfKey && <PrimaryKeyLabel />}
                    {expandedField.isPartitioningKey && <PartitioningKeyLabel />}
                    {expandedField.nullable && <NullableLabel />}
                    <FieldPath displayName={displayName} setExpandedDrawerFieldPath={setExpandedDrawerFieldPath} />
                </TitleWrapper>
                <FieldText>Column</FieldText>
            </NameTypesWrapper>
            <RightGroup>
                <MenuWrapper>
                    <MenuColumn field={expandedField} />
                </MenuWrapper>
                <CloseIcon onClick={() => setExpandedDrawerFieldPath(null)}>
                    <CloseOutlined />
                </CloseIcon>
            </RightGroup>
        </FieldHeaderWrapper>
    );
}
