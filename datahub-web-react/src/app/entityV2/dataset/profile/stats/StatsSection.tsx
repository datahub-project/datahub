import { Divider, Heading } from '@components';
import React from 'react';
import styled from 'styled-components';

const Section = styled.div`
    padding-top: 24px;
    padding-bottom: 40px;
    margin-bottom: 20px;
    width: 100%;
`;

const HeaderRow = styled.div`
    display: flex;
    flex-flow: row wrap;
    justify-content: space-between;
`;

// Keeps the bottom margin antd's level-3 Title had.
const TitleWrapper = styled.div`
    margin-bottom: 12px;
`;

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

type Props = {
    children: React.ReactNode;
    title: string;
    rightFloatView?: React.ReactNode;
};

export default function StatsSection({ children, title, rightFloatView }: Props) {
    return (
        <Section>
            <HeaderRow>
                <TitleWrapper>
                    <Heading type="h3" size="4xl" weight="semiBold">
                        {title}
                    </Heading>
                </TitleWrapper>
                {rightFloatView || <span />}
            </HeaderRow>
            <ThinDivider />
            {children}
        </Section>
    );
}
