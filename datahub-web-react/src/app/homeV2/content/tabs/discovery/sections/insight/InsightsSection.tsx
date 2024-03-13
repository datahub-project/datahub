import React from 'react';
import styled from 'styled-components';
import { Section } from '../Section';
import { useInsightStatusContext } from './InsightStatusProvider';
import { Carousel } from '../../../../../../shared/carousel/Carousel';

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
                <Carousel>{children}</Carousel>
            </Section>
        </Container>
    );
};
