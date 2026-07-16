import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { truncate } from '@app/entityV2/shared/utils';
import { capitalizeFirstLetter } from '@app/shared/textUtil';
import { ColumnTypeIcon } from '@app/sharedV2/utils';

import { SchemaFieldDataType } from '@types';

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
