import React from 'react';
import styled from 'styled-components';

import { Actions } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/Actions';
import { CloseButton } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/CloseButton';
import { AssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/AssertionDescription';
import { AssertionResultPill } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/AssertionResultPill';
import { AssertionSourceType, AssertionType } from '@src/types.generated';

import { Assertion, AssertionResult, DataContract, Monitor } from '@types';

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
    monitor?: Monitor;
    contract?: DataContract | null;
    result?: AssertionResult;
    canEditAssertion: boolean;
    canEditMonitor: boolean;
    canEditContract: boolean;
    close: () => void;
    refetch: () => void;
};

// TODO: Add support for V2 styled actions: Delete, start, stop.
// TODO: Replace with the newer close Icon.
export const AssertionProfileHeader = ({
    assertion,
    monitor,
    contract,
    result,
    canEditAssertion,
    canEditMonitor,
    canEditContract,
    close,
    refetch,
}: Props) => {
    const isFieldAssertion = assertion?.info?.type === AssertionType.Field;
    const isSmartAssertion = assertion.info?.source?.type === AssertionSourceType.Inferred;
    return (
        <>
            <NavBar>
                <CloseButton close={close} />
                <ActionsWrapper>
                    <Actions
                        assertion={assertion}
                        monitor={monitor}
                        contract={contract}
                        canEditAssertion={canEditAssertion}
                        canEditMonitor={canEditMonitor}
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
                            monitor={monitor}
                            options={{
                                noSecondarySpacing: isFieldAssertion,
                                showColumnTag: isFieldAssertion,
                            }}
                        />
                    )) ||
                        'Assertion details'}
                </Title>
                <Status>
                    <AssertionResultPill result={result} isSmartAssertion={isSmartAssertion} />
                </Status>
            </Container>
        </>
    );
};
