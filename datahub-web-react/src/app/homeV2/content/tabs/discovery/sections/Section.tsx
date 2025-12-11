/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const Title = styled.div`
    color: #403d5c;
    margin: 0px;
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 8px;
`;

const Content = styled.div`
    margin-bottom: 20px;
    position: relative;
    &:hover {
        .hover-btn {
            display: flex;
        }
    }
`;

const Action = styled.div`
    color: ${ANTD_GRAY[8]};
    font-size: 12px;
    font-weight: 700;
    :hover {
        cursor: pointer;
        text-decoration: underline;
    }
    white-space: nowrap;
`;

type Props = {
    title: string;
    tip?: string;
    children: React.ReactNode;
    actionText?: string;
    onClickAction?: () => void;
};

export const Section = ({ title, tip, actionText, onClickAction, children }: Props) => {
    return (
        <>
            <Header>
                <Tooltip title={tip}>
                    <Title>{title}</Title>
                </Tooltip>
                {actionText && <Action onClick={onClickAction}>{actionText}</Action>}
            </Header>
            <Content>{children}</Content>
        </>
    );
};
