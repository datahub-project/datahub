import { CloseOutlined } from '@ant-design/icons';
import { Divider } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';
import NullableLabel, {
    PartitioningKeyLabel,
    PrimaryKeyLabel,
} from '@app/entityV2/shared/tabs/Dataset/Schema/components/ConstraintLabels';
import MenuColumn from '@app/entityV2/shared/tabs/Dataset/Schema/components/MenuColumn';
import FieldPath from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldPath';
import TypeLabel from '@app/entityV2/shared/tabs/Dataset/Schema/components/TypeLabel';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { SchemaField } from '@types';

const FieldHeaderWrapper = styled.div`
    padding: 16px;
    display: flex;
    justify-content: space-between;
    border-bottom: 1px solid ${colors.gray[100]};
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
