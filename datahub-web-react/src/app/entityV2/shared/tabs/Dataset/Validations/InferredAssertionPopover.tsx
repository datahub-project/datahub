import React from 'react';
import styled from 'styled-components';
import { Popover, Typography } from 'antd';
import { ThunderboltFilled } from '@ant-design/icons';
import { REDESIGN_COLORS } from '../../../constants';

const Container = styled.div`
    height: 100%;
`;

const Title = styled(Typography.Title)`
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

const Logo = styled(ThunderboltFilled)`
    color: ${REDESIGN_COLORS.BLUE};
    margin-right: 4px;
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
