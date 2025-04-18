import React from 'react';
import styled from 'styled-components';
import { groupTestsByCategory } from './utils';
import { TestsSection } from './TestsSection';
import { Test } from '../../types.generated';
import EmptyTests from './EmptyTests';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => (props.$isShowNavBarRedesign ? 'white' : 'white')};
    ${(props) => !props.$isShowNavBarRedesign && 'min-height: 100vh;'}
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        overflow-y: auto;
        height: calc(100% - 150px);
        border-radius: 0 0 ${props.theme.styles['border-radius-navbar-redesign']} ${props.theme.styles['border-radius-navbar-redesign']};
    `}
`;

type Props = {
    tests: Test[];
};

export const Tests = ({ tests }: Props) => {
    const hasTests = tests.length > 0;
    const testSections = groupTestsByCategory(tests);
    const isShowNavBarRedesign = useShowNavBarRedesign();
    return (
        <Container $isShowNavBarRedesign={isShowNavBarRedesign}>
            {(hasTests &&
                testSections.map((section) => {
                    return (
                        <TestsSection
                            title={section.name}
                            tooltip={section.description}
                            tests={section.tests}
                            key={section.name || ''}
                        />
                    );
                })) || <EmptyTests />}
        </Container>
    );
};
