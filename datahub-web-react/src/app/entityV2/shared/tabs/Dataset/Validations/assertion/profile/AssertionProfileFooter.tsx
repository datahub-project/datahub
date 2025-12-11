/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BellTwoTone } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

const Container = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0px 24px;
    margin-bottom: 20px;
`;

const Tip = styled.div`
    background-color: ${ANTD_GRAY[3]};
    padding: 20px;
    border-radius: 4px;
    color: ${ANTD_GRAY[8]};
`;

const Title = styled.div`
    font-weight: 700;
    margin-bottom: 8px;
`;

const StyledBell = styled(BellTwoTone)`
    margin-right: 4px;
`;

// TODO: Add support for V2 styled actions: Delete, start, stop.
export const AssertionProfileFooter = () => {
    return (
        <Container>
            <Tip>
                <Title>
                    <StyledBell /> When things go wrong, be the first to know.
                </Title>
                Sign up to receive notifications when this assertion passes or fails by{' '}
                <a
                    href="https://docs.datahub.com/docs/next/managed-datahub/subscription-and-notification/"
                    target="_blank"
                    rel="noreferrer noopener"
                >
                    subscribing to this table.
                </a>
            </Tip>
        </Container>
    );
};
