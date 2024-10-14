import { CloseOutlined } from '@ant-design/icons';
import { useAppConfig } from '@app/useAppConfig';
import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { SchemaField } from '../../../../../../../../types.generated';
import translateFieldPath from '../../../../../../dataset/profile/schema/utils/translateFieldPath';
import { REDESIGN_COLORS } from '../../../../../constants';
import NullableLabel, { PartitioningKeyLabel, PrimaryKeyLabel } from '../ConstraintLabels';
import MenuColumn from '../MenuColumn';
import TypeLabel from '../TypeLabel';
import FieldPath from './FieldPath';
import { useEntityRegistry } from '../../../../../../../useEntityRegistry';

const FieldHeaderWrapper = styled.div`
    padding: 16px;
    display: flex;
    justify-content: space-between;
    background: ${REDESIGN_COLORS.BACKGROUND_PURPLE};
`;

const NameTypesWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const TitleWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 5px;

    color: ${REDESIGN_COLORS.WHITE_WIRE};
    font-size: 16px;
    font-weight: bold;
`;

const RightGroup = styled.div`
    display: flex;
    align-items: baseline;
`;

const MenuWrapper = styled.div`
    color: ${REDESIGN_COLORS.WHITE};
    margin-right: 5px;
`;

const FieldText = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.WHITE};
    opacity: 0.5;
    font-size: 12px;
    font-weight: 700;
    line-height: 24px;
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
        stroke: ${REDESIGN_COLORS.WHITE};
        stroke-width: 10px;
    }

    svg {
        height: 16px;
        width: 16px;
        color: ${REDESIGN_COLORS.WHITE};
    }
`;

const VerticalDivider = styled.hr`
    align-self: center;
    border: 1px solid;
    height: 75%;
    margin: 0;
`;

const StyledTypeLabel = styled(TypeLabel)`
    font-size: 14px;
`;

const StyleLink = styled(Link)`
    color: ${REDESIGN_COLORS.WHITE};

    &:hover {
        color: ${REDESIGN_COLORS.WHITE};
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
                <FieldText>Field</FieldText>
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
                    <VerticalDivider />
                    <StyledTypeLabel type={expandedField.type} nativeDataType={expandedField.nativeDataType} />
                    {expandedField.isPartOfKey && <PrimaryKeyLabel />}
                    {expandedField.isPartitioningKey && <PartitioningKeyLabel />}
                    {!expandedField.nullable && <NullableLabel />}
                    <FieldPath displayName={displayName} setExpandedDrawerFieldPath={setExpandedDrawerFieldPath} />
                </TitleWrapper>
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
