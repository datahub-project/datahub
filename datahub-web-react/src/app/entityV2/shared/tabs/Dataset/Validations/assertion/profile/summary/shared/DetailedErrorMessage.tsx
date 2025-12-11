/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { getDetailedErrorMessage } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultMessageUtils';

import { AssertionRunEvent } from '@types';

const Container = styled.div`
    padding: 4px;
`;

const Row = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    padding: 4px 8px;
`;

const Title = styled.div`
    font-weight: bold;
    margin-right: 8px;
    font-size: 16px;
`;

const Message = styled.div`
    color: ${ANTD_GRAY[8]};
`;

type Props = {
    run: AssertionRunEvent;
};

export const DetailedErrorMessage = ({ run }: Props) => {
    const type = run?.result?.error?.type;
    const message = getDetailedErrorMessage(run);
    return (
        <Container>
            <Row>
                <Title>{type} </Title>
            </Row>
            {message && (
                <Row>
                    <Message>{message}</Message>
                </Row>
            )}
        </Container>
    );
};
