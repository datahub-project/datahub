import React from 'react';

import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../../../constants';

const Title = styled.div`
    padding: 0;
    margin: 0;
    margin-bottom: 4px;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[9]};
    font-weight: 600;
    font-size: 16px;
    margin-bottom: 18px;
`;

type Props = {
    title?: string;
    children: React.ReactNode;
};

export const AssertionSummarySection = ({ title, children }: Props) => {
    return (
        <>
            {title ? <Title>{title}</Title> : null}
            {children}
        </>
    );
};
