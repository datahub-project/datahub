import { GlobalOutlined, LockOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DataHubViewType } from '@types';

const StyledLockOutlined = styled(LockOutlined)<{ color }>`
    color: ${(props) => props.color};
    margin-right: 4px;
`;

const StyledGlobalOutlined = styled(GlobalOutlined)<{ color }>`
    color: ${(props) => props.color};
    margin-right: 4px;
`;

const StyledText = styled(Typography.Text)<{ color }>`
    && {
        color: ${(props) => props.color};
    }
`;

type Props = {
    type: DataHubViewType;
    color: string;
};

/**
 * Label used to describe View Types
 *
 * @param param0 the color of the text and iconography
 */
export const ViewTypeLabel = ({ type, color }: Props) => {
    const { t } = useTranslation('entity.views');
    const copy =
        type === DataHubViewType.Personal ? (
            <Trans t={t} i18nKey="viewType.privateDescription" components={{ bold: <b /> }} />
        ) : (
            <Trans t={t} i18nKey="viewType.publicDescription" components={{ bold: <b /> }} />
        );
    const Icon = type === DataHubViewType.Global ? StyledGlobalOutlined : StyledLockOutlined;

    return (
        <>
            <Icon color={color} />
            <StyledText color={color} type="secondary">
                {copy}
            </StyledText>
        </>
    );
};
