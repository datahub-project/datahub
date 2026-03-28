import { Tooltip } from '@components';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { TruncatedTextWithTooltip } from '@app/shared/TruncatedTextWithTooltip';

const Container = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const LeftColumn = styled.div`
    flex: 1;
`;

const RightColumn = styled.div`
    padding: 12px;
`;

const Title = styled.div`
    padding: 0;
    margin: 0;
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    color: ${(props) => props.theme.colors.textSecondary};
    font-weight: 700;
`;

const StyledTruncatedText = styled(TruncatedTextWithTooltip)`
    color: ${(props) => props.theme.colors.textTertiary};
`;

const StyledDivider = styled(Divider)`
    margin: 16px 0px;
`;

type Props = {
    icon?: React.ReactNode;
    title: string;
    subtitle: string;
    tooltip?: React.ReactNode;
    showDivider: boolean;
    action?: React.ReactNode;
};

/**
 * Renders a given section of the summary.
 */
export const AssertionScheduleSummarySection = ({ icon, title, subtitle, tooltip, showDivider, action }: Props) => {
    return (
        <>
            <Container>
                <LeftColumn>
                    <Tooltip placement="topLeft" title={tooltip} showArrow={false}>
                        <Title>
                            {icon}
                            {title}
                        </Title>
                    </Tooltip>
                    <StyledTruncatedText text={subtitle} maxLength={200} />
                </LeftColumn>
                <RightColumn>{action}</RightColumn>
            </Container>
            {showDivider ? <StyledDivider dashed /> : null}
        </>
    );
};
