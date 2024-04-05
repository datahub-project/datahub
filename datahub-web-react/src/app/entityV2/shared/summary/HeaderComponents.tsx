import React from 'react';
import styled from 'styled-components';
import { ReadOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../constants';

export const SUMMARY_TAB_ICON = ReadOutlined;

export const SummaryTabWrapper = styled.div`
    display: flex;
    flex-direction: column;
    height: fit-content;
    padding: 12px 20px;
    gap: 20px;
    position: relative;
`;

export const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 20px;

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

export const HeaderTitle = styled.h3`
    align-items: center;
    display: flex;
    color: ${ANTD_GRAY[9]};
    font-size: 16px;
    font-weight: 600;
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
