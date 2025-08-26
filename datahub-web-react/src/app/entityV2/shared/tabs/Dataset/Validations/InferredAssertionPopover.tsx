import { Popover } from '@components';
import { Typography } from 'antd';
import { Sparkle } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const Container = styled.div``;

const Title = styled(Typography.Title)`
    display: flex;
    align-items: center;
    gap: 8px;
    && {
        margin: 0px;
        padding-top: 4px;
        padding-bottom: 4px;
    }
`;

const Description = styled.div``;

const Section = styled.div`
    margin-bottom: 8px;
`;

const Logo = styled(Sparkle)`
    color: ${REDESIGN_COLORS.BLUE};
`;
const popoverStyle = { maxWidth: 280, border: `1px solid ${REDESIGN_COLORS.BLUE}` };

type Props = {
    children: React.ReactNode;
};

export const InferredAssertionPopover = ({ children }: Props) => {
    return (
        <Popover
            showArrow={false}
            overlayInnerStyle={popoverStyle}
            trigger="hover"
            title={
                <Title level={5}>
                    <Logo />
                    Smart Assertion
                </Title>
            }
            content={
                <Description>
                    <Section>
                        This is an ML-driven <b>dynamic assertion</b> that is automatically generated based on the
                        patterns observed for this dataset.
                    </Section>
                    <Section>
                        Notice that this assertion may change to accommodate new trends observed for this dataset.
                    </Section>
                </Description>
            }
        >
            <Container>{children}</Container>
        </Popover>
    );
};
