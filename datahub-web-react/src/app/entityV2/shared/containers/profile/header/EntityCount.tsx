import { Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { formatNumber } from '@app/shared/formatNumber';

const EntityCountText = styled(Typography.Text)`
    display: inline-block;
    font-size: 12px;
    line-height: 20px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textTertiary};
`;

interface Props {
    entityCount?: number;
    displayAssetsText?: boolean;
}

function EntityCount(props: Props) {
    const { t } = useTranslation('entity.shared.containers');
    const { entityCount, displayAssetsText } = props;

    return (
        <EntityCountText className="entityCount">
            {displayAssetsText
                ? t('entityCount.asset', { count: entityCount ?? 0, formattedCount: formatNumber(entityCount ?? 0) })
                : t('entityCount.entity', { count: entityCount ?? 0, formattedCount: formatNumber(entityCount ?? 0) })}
        </EntityCountText>
    );
}

export default EntityCount;
