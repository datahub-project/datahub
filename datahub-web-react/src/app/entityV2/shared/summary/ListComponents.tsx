import React from 'react';
import styled from 'styled-components';

export const HorizontalList = styled.div<{ gap?: number }>`
    display: flex;
    flex-direction: row;
    gap: ${({ gap }) => gap || 12}px;
    padding-right: min(10%, 40px);
    overflow-x: auto;
    overflow-y: hidden;
    scroll-behavior: smooth;
    white-space: nowrap;
    width: 100%;

    &::-webkit-scrollbar {
        display: none;
    }
`;

const SummaryColumnsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 10px;
    max-width: 100%;
    overflow: hidden;
    white-space: nowrap;
    width: 100%;
`;

const SummaryColumn = styled.div<{ n: number }>`
    display: flex;
    flex-direction: column;
    flex-grow: 1;
`;

const HorizontalDivider = styled.hr<{ margin: number }>`
    align-self: stretch;
    height: 1px;
    margin: 10px ${({ margin }) => margin}px;
    opacity: 0.2;
`;

export function SummaryColumns({ children }: { children: React.ReactNode[] }) {
    return (
        <SummaryColumnsWrapper>
            {children
                .filter((c) => !!c)
                .map((child, i) => (
                    <>
                        {i !== 0 && <HorizontalDivider margin={0} />}
                        <SummaryColumn n={children.length}>{child}</SummaryColumn>
                    </>
                ))}
        </SummaryColumnsWrapper>
    );
}
