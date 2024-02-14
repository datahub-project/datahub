import React from 'react';
import styled from 'styled-components';
import { Section } from '../Section';
import { useInsightStatusContext } from './InsightStatusProvider';

export const List = styled.div`
    display: flex;
    flex-direction: row;
    overflow: auto;
    margin-bottom: 4px;
    margin-top: 12px;
    gap: 16px;
    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
    padding-right: 40px;
    mask-image: linear-gradient(to right, rgba(0, 0, 0, 1) 90%, rgba(255, 0, 0, 0.5) 99%, rgba(255, 0, 0, 0) 100%);
`;

const Container = styled.div<{ hide: boolean }>`
    ${(props) => props.hide && 'display: none;'}
`;

type Props = {
    children: React.ReactNode;
};

export const InsightsSection = ({ children }: Props) => {
    const { insightStatuses } = useInsightStatusContext();
    const hasInsights = Array.from(insightStatuses.values()).some((status) => status);
    return (
        <Container hide={!hasInsights}>
            <Section title="For you">
                <List>{children}</List>
            </Section>
        </Container>
    );
};
