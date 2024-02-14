import { Popover, Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';
import TypeLabel from '../../../../shared/tabs/Dataset/Schema/components/TypeLabel';
import { ExtendedSchemaFields } from './types';
import { ColumnTypeIcon, TypeTooltipTitle } from '../../../../../sharedV2/utils';
import { REDESIGN_COLORS } from '../../../../shared/constants';

const FieldTypeWrapper = styled.div`
    display: inline-flex;
    align-items: center;
`;
const FieldTypeContainer = styled.div`
    vertical-align: top;
    display: flex;
`;

const TypeWrapper = styled.div<{ nullable: boolean }>`
    margin-right: 4px;
    border: ${(props) => (props.nullable ? 'none' : '1px solid')};
    border-color: ${REDESIGN_COLORS.DARK_GREY};
    border-radius: 50%;
    width: 16px;
    height: 16px;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 12px;
    svg {
        height: 14px;
        width: 14px;
        color: ${REDESIGN_COLORS.DARK_GREY};
    }
`;

type InteriorTypeProps = {
    record: ExtendedSchemaFields;
    showTypeAsIcons: boolean;
};

const InteriorTypeContent = ({ record, showTypeAsIcons }: InteriorTypeProps) => {
    return (
        <FieldTypeWrapper>
            <FieldTypeContainer>
                {!showTypeAsIcons && <TypeLabel type={record.type} nativeDataType={record.nativeDataType} />}

                {!!showTypeAsIcons && record.type && (
                    <TypeWrapper nullable={record.nullable} className="field-type">
                        <Tooltip
                            showArrow={false}
                            placement="left"
                            title={TypeTooltipTitle(record.type, record.nativeDataType)}
                        >
                            {ColumnTypeIcon(record.type)}
                        </Tooltip>
                    </TypeWrapper>
                )}
            </FieldTypeContainer>
        </FieldTypeWrapper>
    );
};

export default function useSchemaTypeRenderer(showTypeAsIcons: boolean) {
    return (fieldPath: string, record: ExtendedSchemaFields): JSX.Element => {
        return (
            <>
                <Popover content={<InteriorTypeContent record={record} showTypeAsIcons={showTypeAsIcons} />}>
                    <InteriorTypeContent record={record} showTypeAsIcons={showTypeAsIcons} />
                </Popover>
            </>
        );
    };
}
