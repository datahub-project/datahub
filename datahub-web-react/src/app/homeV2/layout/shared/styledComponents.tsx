import React from 'react';
import styled from 'styled-components';

const ReferenceSectionContainer = styled.div`
    padding: 0px 12px 0px 12px;
    overflow: wrap;
`;

export const ReferenceSectionDivider = styled.hr`
    height: 1px;
    opacity: 0.1;
    width: 100%;
    margin: 20px 0px;
`;

export const HorizontalList = styled.div`
    display: flex;
    flex-direction: row;
    overflow: auto;
    margin-bottom: 4px;
    gap: 10px;
    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
    padding-right: 40px;
    mask-image: linear-gradient(to right, rgba(0, 0, 0, 1) 90%, rgba(255, 0, 0, 0.5) 99%, rgba(255, 0, 0, 0) 100%);
`;

export const ReferenceSection = ({ children }: { children: React.ReactNode }) => {
    return (
        <ReferenceSectionContainer>
            {children}
            <ReferenceSectionDivider />
        </ReferenceSectionContainer>
    );
};
