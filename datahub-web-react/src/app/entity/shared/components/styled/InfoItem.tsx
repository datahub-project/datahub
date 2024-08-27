import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../../constants';

const HeaderInfoItem = styled.div<{ onClick?: () => void; width?: string }>`
    display: inline-block;
    text-align: left;
    width: ${(props) => (props.width ? `${props.width};` : '125px;')};
    vertical-align: top;
    &:hover {
        cursor: ${(props) => (props.onClick ? 'pointer' : 'default')};
    }
`;

const HeaderInfoTitle = styled(Typography.Text)`
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    title: string;
    children: React.ReactNode;
    onClick?: () => void;
    width?: string;
};

export const InfoItem = ({ title, children, width, onClick }: Props) => {
    return (
        <HeaderInfoItem onClick={onClick} width={width}>
            <div>
                <HeaderInfoTitle>{title}</HeaderInfoTitle>
            </div>
            <span>{children}</span>
        </HeaderInfoItem>
    );
};
