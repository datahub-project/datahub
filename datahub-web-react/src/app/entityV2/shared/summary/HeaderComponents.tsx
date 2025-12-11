/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BookOpen } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

export const SUMMARY_TAB_ICON = BookOpen;

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
