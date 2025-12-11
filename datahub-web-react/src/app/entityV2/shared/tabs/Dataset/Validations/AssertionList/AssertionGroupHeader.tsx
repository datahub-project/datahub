/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { AssertionGroup } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylTypes';

const Container = styled.div`
    display: flex;
    align-items: center;
    padding: 4px 0px;
    &:hover {
        cursor: pointer;
    }
`;

const TextContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
    font-size: 14px;
`;

const Title = styled(Typography.Text)`
    && {
        padding-bottom: 0px;
        margin-bottom: 0px;
    }
`;

const Message = styled(Typography.Text)`
    && {
        font-size: 12px;
        margin-left: 8px;
    }
`;

type Props = {
    group: AssertionGroup;
};

export const AssertionGroupHeader = ({ group }: Props) => {
    const { summary } = group;
    const inactiveCount = summary.totalAssertions - summary.total;
    const summaryMessage = `${summary.passing} passing, ${summary.failing} failing${
        summary.erroring ? `, ${summary.erroring} errors` : ''
    }${inactiveCount ? `, ${inactiveCount} inactive` : ''}`;
    return (
        <Container>
            <TextContainer>
                <Title strong>{group.name}</Title>
                <Message type="secondary">{summaryMessage}</Message>
            </TextContainer>
        </Container>
    );
};
