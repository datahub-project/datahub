import React from 'react';

import styled from 'styled-components';

import { AssertionRunEvent } from '../../../../../../../../../../types.generated';
import { getDetailedErrorMessage } from './resultMessageUtils';
import { ANTD_GRAY } from '../../../../../../../constants';

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
