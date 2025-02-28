import React from 'react';
import styled from 'styled-components';
import { ReadOutlined } from '@ant-design/icons';
import { REDESIGN_COLORS } from '../constants';

export const SUMMARY_TAB_ICON = ReadOutlined;

export const SummaryTabWrapper = styled.div`
    display: flex;
    flex-direction: column;
    height: fit-content;
    padding: 12px 20px;
    gap: 20px;
    position: relative;
`;

export const SectionContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;

    &:hover {
        .hover-btn {
            display: flex;
        }
    }
`;

export const SummaryTabHeaderWrapper = styled.div`
    margin-top: 4px;
    align-items: center;
    display: flex;
    justify-content: space-between;
`;

export const SummaryHeaderButtonGroup = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
`;

export const HeaderTitle = styled.h3`
    align-items: center;
    display: flex;
    font-size: 18px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-weight: 500;
    margin: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    text-transform: capitalize;
    white-space: nowrap;

    img,
    svg {
        margin-right: 8px;
    }
`;

export function SummaryTabHeaderTitle({ icon, title }: { icon?: React.ReactNode; title: string }) {
    return (
        <HeaderTitle>
            {React.isValidElement(icon) && icon}
            {title}
        </HeaderTitle>
    );
}
