import { Badge, Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { truncate } from '@app/entity/shared/utils';
import { capitalizeFirstLetter } from '@app/shared/textUtil';

import { SchemaFieldDataType } from '@types';

type Props = {
    type: SchemaFieldDataType;
    nativeDataType: string | null | undefined;
};

const TypeBadge = styled(Badge)`
    margin-left: 4px;
    &&& .ant-badge-count {
        background-color: ${ANTD_GRAY[1]};
        color: ${ANTD_GRAY[7]};
        border: 1px solid ${ANTD_GRAY[5]};
        font-size: 12px;
        font-weight: 400;
        height: 22px;
    }
`;

export default function TypeLabel({ type, nativeDataType }: Props) {
    // if unable to match type to DataHub, display native type info by default
    const nativeFallback = type === SchemaFieldDataType.Null;

    // eslint-disable-next-line react/prop-types
    const NativeDataTypeTooltip = ({ children }) =>
        nativeDataType ? (
            <Tooltip placement="top" title={capitalizeFirstLetter(nativeDataType)}>
                <span>{children}</span>
            </Tooltip>
        ) : (
            <>{children}</>
        );

    return (
        <NativeDataTypeTooltip>
            <TypeBadge count={capitalizeFirstLetter(nativeFallback ? truncate(250, nativeDataType) : type)} />
        </NativeDataTypeTooltip>
    );
}
