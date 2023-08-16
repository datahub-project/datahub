import React, { useState } from 'react';
import styled from 'styled-components';
import { CheckCircleFilled, CloseOutlined, StopOutlined } from '@ant-design/icons';
import { Button, message, Typography } from 'antd';
import { useRunTestDefinitionMutation } from '../../../../../graphql/test.generated';
import { RunTestDefinitionStatus } from '../../../../../types.generated';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { FAILURE_COLOR_HEX, SUCCESS_COLOR_HEX } from '../../../../entity/shared/tabs/Incident/incidentUtils';
import { EntityActionProps } from '../../../../entity/shared/components/styled/search/EntitySearchResults';
import { TestBuilderState } from '../../types';

const StatusContainer = styled.div<{ color?: string }>`
    display: flex;
    align-items: center;
    justify-content: right;
    padding-left: 12px;
    padding-top: 8px;
    padding-bottom: 8px;
    padding-right: 12px;
    width: 140px;
`;

const StatusText = styled(Typography.Text)`
    margin: 0px;
    padding: 0px;
    margin-left: 8px;
    font-size: 14px;
`;

const getStatusView = (status) => {
    switch (status) {
        case RunTestDefinitionStatus.Pass:
            return (
                <StatusContainer color={SUCCESS_COLOR_HEX}>
                    <CheckCircleFilled style={{ fontSize: 14, color: SUCCESS_COLOR_HEX }} />
                    <StatusText style={{ color: SUCCESS_COLOR_HEX }} strong>
                        Passed
                    </StatusText>
                </StatusContainer>
            );
        case RunTestDefinitionStatus.Fail:
            return (
                <StatusContainer color={FAILURE_COLOR_HEX}>
                    <CloseOutlined style={{ fontSize: 14, color: FAILURE_COLOR_HEX }} />
                    <StatusText style={{ color: FAILURE_COLOR_HEX }}>Failed</StatusText>
                </StatusContainer>
            );
        case RunTestDefinitionStatus.None:
            return (
                <StatusContainer color={ANTD_GRAY[5]}>
                    <StopOutlined style={{ fontSize: 14, color: ANTD_GRAY[5] }} />
                    <StatusText style={{ color: ANTD_GRAY[5] }}>Not selected</StatusText>
                </StatusContainer>
            );
        default:
            return <>N / A</>;
    }
};

export const getValidateEntityAction = (state: TestBuilderState) => {
    return ({ urn }: EntityActionProps) => {
        const [status, setStatus] = useState<RunTestDefinitionStatus | undefined>(undefined);
        const [runTestDefinitionMutation] = useRunTestDefinitionMutation();

        const onExecuteTest = () => {
            runTestDefinitionMutation({
                variables: {
                    urn,
                    test: {
                        json: state.definition?.json,
                    },
                },
            })
                .then((result) => {
                    if (result) {
                        const s = result.data?.runTestDefinition.status;
                        setStatus(s);
                    }
                })
                .catch(() => {
                    message.destroy();
                    message.warn('Failed to run test against this entity! Please check your test definition.');
                });
        };

        return (
            <>
                {(status && getStatusView(status)) || (
                    <Button onClick={onExecuteTest} type="link">
                        Run Test
                    </Button>
                )}
            </>
        );
    };
};
