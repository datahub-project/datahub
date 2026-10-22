import { Badge } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { TypeData } from '@app/entity/shared/tabs/Properties/types';
import { truncate } from '@app/entity/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { DataTypeEntity, SchemaFieldDataType } from '@types';

type Props = {
    type: TypeData;
    dataType?: DataTypeEntity;
    displayTransparent?: boolean;
};

export const PropertyTypeBadge = styled(Badge)<{ displayTransparent?: boolean }>`
    margin: 4px 0 4px 8px;
    &&& .ant-badge-count {
        ${(props) =>
            props.displayTransparent
                ? `
            background-color: transparent;
            `
                : `
        background-color: ${props.theme.colors.bg};
        color: ${props.theme.colors.textSecondary};
        border: 1px solid ${props.theme.colors.border};
        `}
        font-size: 12px;
        font-weight: 500;
        font-family: 'Manrope';
    }
`;

export default function PropertyTypeLabel({ type, dataType, displayTransparent }: Props) {
    // if unable to match type to DataHub, display native type info by default
    const { nativeDataType } = type;
    const nativeFallback = type.type === SchemaFieldDataType.Null;

    const typeText =
        dataType?.info?.displayName ||
        dataType?.info?.type ||
        (nativeFallback ? truncate(250, nativeDataType) : type.type);

    return <PropertyTypeBadge count={capitalizeFirstLetterOnly(typeText)} displayTransparent={displayTransparent} />;
}
