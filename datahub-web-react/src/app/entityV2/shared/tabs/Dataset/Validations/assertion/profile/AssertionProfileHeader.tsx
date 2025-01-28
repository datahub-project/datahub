import React from 'react';

import styled from 'styled-components';
import { AssertionType } from '@src/types.generated';

import { Assertion, AssertionResult, DataContract } from '../../../../../../../../types.generated';
import { AssertionDescription } from './summary/AssertionDescription';
import { AssertionResultPill } from './summary/shared/AssertionResultPill';
import { Actions } from './actions/Actions';
import { CloseButton } from './shared/CloseButton';

const Container = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: start;
    padding: 20px 24px;
    border-bottom: 1px solid #f0f0f0;
    background-color: #fff;
`;

const NavBar = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid #f0f0f0;
`;

const Status = styled.div``;

const ActionsWrapper = styled.div`
    padding: 8px 0px;
`;

const Title = styled.div`
    flex: 1;
    font-size: 16px;
    font-weight: 600;
    margin-right: 16px;
    . * {
        text-overflow: ellipsis;
    }
`;

type Props = {
    assertion: Assertion;
    contract?: DataContract;
    result?: AssertionResult;
    canEditContract: boolean;
    close: () => void;
    refetch: () => void;
};

// TODO: Add support for V2 styled actions: Delete, start, stop.
// TODO: Replace with the newer close Icon.
export const AssertionProfileHeader = ({ assertion, contract, result, canEditContract, close, refetch }: Props) => {
    const isFieldAssertion = assertion?.info?.type === AssertionType.Field;
    return (
        <>
            <NavBar>
                <CloseButton close={close} />
                <ActionsWrapper>
                    <Actions
                        assertion={assertion}
                        contract={contract}
                        canEditContract={canEditContract}
                        refetch={refetch}
                    />
                </ActionsWrapper>
            </NavBar>
            <Container>
                <Title>
                    {(assertion && (
                        <AssertionDescription
                            assertion={assertion}
                            options={{
                                noSecondarySpacing: isFieldAssertion,
                                showColumnTag: isFieldAssertion,
                            }}
                        />
                    )) ||
                        'Assertion details'}
                </Title>
                <Status>
                    <AssertionResultPill result={result} />
                </Status>
            </Container>
        </>
    );
};
