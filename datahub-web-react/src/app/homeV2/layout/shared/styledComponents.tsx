import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import React from 'react';
import styled from 'styled-components';

const ReferenceSectionContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) => !props.$isShowNavBarRedesign && 'padding: 0px 12px 0px 12px;'}
    overflow: wrap;
`;

export const ReferenceSectionDivider = styled.hr`
    height: 1px;
    opacity: 0.1;
    width: 100%;
    margin: 20px 0px;
`;

export const ReferenceSection = ({ children }: { children: React.ReactNode }) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    return (
        <ReferenceSectionContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            {children}
            <ReferenceSectionDivider />
        </ReferenceSectionContainer>
    );
};
