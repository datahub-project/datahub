import React from 'react';
import styled from 'styled-components';
import { ArrowRightOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { Assertion } from '../../../../../../../types.generated';
import { StyledCheckOutlined, StyledCloseOutlined, StyledExclamationOutlined } from '../shared/styledComponents';
import { getAssertionsSummary } from '../acrylUtils';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../../constants';

const Container = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const StatusContainer = styled.div`
    display: flex;
    align-items: center;
`;

const StatusText = styled.div`
    color: ${ANTD_GRAY[8]};
    margin-left: 4px;
`;

const ActionButton = styled(Button)`
    color: ${REDESIGN_COLORS.BLUE};
`;

const StyledArrowRightOutlined = styled(ArrowRightOutlined)`
    font-size: 8px;
`;

type Props = {
    assertions: Assertion[];
    passingText: string;
    failingText: string;
    errorText: string;
    actionText?: string;
    showAction?: boolean;
};

export const DataContractSummaryFooter = ({
    assertions,
    actionText,
    passingText,
    errorText,
    failingText,
    showAction = true,
}: Props) => {
    const summary = getAssertionsSummary(assertions);
    const isFailing = summary.failing > 0;
    const isPassing = summary.passing && summary.passing === summary.total;
    const isErroring = summary.erroring > 0;
    return (
        <Container>
            <StatusContainer>
                {(isFailing && <StyledCloseOutlined />) || undefined}
                {(isPassing && <StyledCheckOutlined />) || undefined}
                {(isErroring && !isFailing && <StyledExclamationOutlined />) || undefined}
                <StatusText>
                    {(isFailing && failingText) || undefined}
                    {(isPassing && passingText) || undefined}
                    {(isErroring && errorText) || undefined}
                </StatusText>
            </StatusContainer>
            {showAction && (
                <ActionButton type="text">
                    {actionText}
                    <StyledArrowRightOutlined />
                </ActionButton>
            )}
        </Container>
    );
};
