import { Badge } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY, ANTD_GRAY_V2 } from '@app/entity/shared/constants';
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
        background-color: ${ANTD_GRAY[1]};
        color: ${ANTD_GRAY_V2[8]};
        border: 1px solid ${ANTD_GRAY_V2[6]};
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
