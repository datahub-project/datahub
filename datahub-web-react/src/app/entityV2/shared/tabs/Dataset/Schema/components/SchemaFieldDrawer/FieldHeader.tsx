import { CloseOutlined } from '@ant-design/icons';
import { Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
// import { ReactComponent as HistoryIcon } from '../../../../../../../../images/history-icon.svg';
import { SchemaField } from '../../../../../../../../types.generated';
import translateFieldPath from '../../../../../../dataset/profile/schema/utils/translateFieldPath';
import { REDESIGN_COLORS } from '../../../../../constants';
import MenuColumn from '../MenuColumn';
// import NullableLabel from '../NullableLabel';
import PartitioningKeyLabel from '../PartitioningKeyLabel';
import PrimaryKeyLabel from '../PrimaryKeyLabel';
import TypeLabel from '../TypeLabel';
import FieldTitle from './FieldTitle';
import { ColumnTypeIcon, TypeTooltipTitle } from '../../../../../../../sharedV2/utils';
import FieldPath from './FieldPath';

const FieldHeaderWrapper = styled.div`
    padding: 16px;
    display: flex;
    justify-content: space-between;
    background: ${REDESIGN_COLORS.BACKGROUND_PURPLE};
`;

const TypesSection = styled.div`
    margin-top: 8px;
    display: flex;
    gap: 5px;
    align-items: center;
    && .ant-badge-count {
        background: transparent !important;
        color: ${REDESIGN_COLORS.WHITE} !important;
        border: 1px solid ${REDESIGN_COLORS.WHITE} !important;
    }
`;

const NameTypesWrapper = styled.div`
    overflow: hidden;
    display: flex;
    gap: 15px;
`;
const NameContainer = styled.div`
    display: flex;
    flex-direction: column;
`;
const TypeContainer = styled.div`
    display: flex;
    align-items: flex-end;
    gap: 5px;
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

// const HistoryText = styled(Typography.Text)`
//     color: ${REDESIGN_COLORS.WHITE};
//     font-family: Manrope;
//     font-size: 12px;
//     font-weight: 600;
//     line-height: 20px;
// `;

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

const TypeWrapper = styled.div<{ nullable: boolean }>`
    margin-right: 4px;
    border: ${(props) => (props.nullable ? 'none' : '1px solid')};
    border-color: ${REDESIGN_COLORS.WHITE};
    border-radius: 50%;
    width: 20px;
    height: 20px;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 14px;
    svg {
        height: 14px;
        width: 14px;
        color: ${REDESIGN_COLORS.WHITE};
    }
`;
// const HistoryContainer = styled.div`
//     display: flex;
//     align-items: center;
// `;
// const VersionHistory = styled.div`
//     padding: 5px 10px;
//     display: flex;
//     gap: 5px;
//     align-items: center;
//     background: #958ebf;
//     border-radius: 100px;
//     color: ${REDESIGN_COLORS.WHITE};
//     font-family: Manrope;
//     font-size: 12px;
//     font-weight: 500;
//     line-height: 8px;
//     letter-spacing: 0.2px;
//     max-height: 30px;

//     svg {
//         height: 16px;
//         width: 16px;
//         color: ${REDESIGN_COLORS.WHITE};
//     }
// `;

interface Props {
    expandedField: SchemaField;
    setExpandedDrawerFieldPath: (fieldPath: string | null) => void;
    showTypeAsIcons?: boolean;
}

export default function FieldHeader({ expandedField, setExpandedDrawerFieldPath, showTypeAsIcons = true }: Props) {
    const displayName = translateFieldPath(expandedField.fieldPath || '');

    return (
        <FieldHeaderWrapper>
            <NameTypesWrapper>
                <NameContainer>
                    <FieldText> Field</FieldText>
                    <FieldTitle displayName={displayName} />
                </NameContainer>
                <TypeContainer>
                    <TypesSection>
                        {!showTypeAsIcons && (
                            <TypeLabel type={expandedField.type} nativeDataType={expandedField.nativeDataType} />
                        )}
                        {!!showTypeAsIcons && expandedField.type && (
                            <TypeWrapper nullable={expandedField.nullable}>
                                <Tooltip
                                    showArrow={false}
                                    placement="left"
                                    title={TypeTooltipTitle(expandedField.type, expandedField.nativeDataType)}
                                >
                                    {ColumnTypeIcon(expandedField.type)}
                                </Tooltip>
                            </TypeWrapper>
                        )}
                        {expandedField.isPartOfKey && <PrimaryKeyLabel />}
                        {expandedField.isPartitioningKey && <PartitioningKeyLabel />}
                        {/* {expandedField.nullable && <NullableLabel />} */}
                    </TypesSection>
                </TypeContainer>
                <FieldPath displayName={displayName} setExpandedDrawerFieldPath={setExpandedDrawerFieldPath} />
            </NameTypesWrapper>
            <RightGroup>
                {/* <HistoryContainer>
                    <VersionHistory>
                        <HistoryText>Version History </HistoryText> <HistoryIcon />
                    </VersionHistory>
                </HistoryContainer> */}
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
