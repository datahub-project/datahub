import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { GlobalOutlined, LockOutlined } from '@ant-design/icons';
import { DataHubViewType } from '../../../types.generated';

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
    onClick?: () => void;
};

/**
 * Label used to describe View Types
 *
 * @param param0 the color of the text and iconography
 */
export const ViewTypeLabel = ({ type, color, onClick }: Props) => {
    const copy =
        type === DataHubViewType.Personal ? (
            <>
                <b>Private</b> - only visible to you.
            </>
        ) : (
            <>
                <b>Public</b> - visible to everyone.
            </>
        );
    const Icon = type === DataHubViewType.Global ? StyledGlobalOutlined : StyledLockOutlined;

    return (
        // eslint-disable-next-line jsx-a11y/click-events-have-key-events,jsx-a11y/no-static-element-interactions
        <div onClick={onClick}>
            <Icon color={color} />
            <StyledText color={color} type="secondary">
                {copy}
            </StyledText>
        </div>
    );
};
