import React from 'react';
import { Tooltip } from '@components';
import styled from 'styled-components';

import { capitalizeFirstLetter } from '../../../../../../shared/textUtil';
import { SchemaFieldDataType } from '../../../../../../../types.generated';
import { ColumnTypeIcon } from '../../../../../../sharedV2/utils';
import { truncate } from '../../../../utils';

const Wrapper = styled.div`
    display: flex;
    align-items: center;
`;

const IconWrapper = styled.div`
    margin-right: 4px;
    font-size: 14px;
    display: flex;
`;

interface Props {
    type: SchemaFieldDataType;
    nativeDataType: string | null | undefined;
    className?: string;
}

export default function TypeLabel({ type, nativeDataType, className }: Props) {
    // if unable to match type to DataHub, display native type info by default
    const nativeFallback = type === SchemaFieldDataType.Null;

    const NativeDataTypeTooltip = ({ children }: { children: React.ReactNode }) =>
        nativeDataType ? (
            <Tooltip placement="top" title={capitalizeFirstLetter(nativeDataType)}>
                {children}
            </Tooltip>
        ) : (
            <>{children}</>
        );

    return (
        <NativeDataTypeTooltip>
            <Wrapper className={className}>
                <IconWrapper>{ColumnTypeIcon(type)}</IconWrapper>
                {capitalizeFirstLetter(nativeFallback ? truncate(250, nativeDataType) : type)}
            </Wrapper>
        </NativeDataTypeTooltip>
    );
}
