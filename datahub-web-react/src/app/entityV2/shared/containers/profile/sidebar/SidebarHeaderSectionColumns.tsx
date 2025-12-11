/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { SidebarStatsColumn } from '@app/entityV2/shared/containers/profile/utils';
import HorizontalScroller from '@app/sharedV2/carousel/HorizontalScroller';

const ColumnsContainer = styled(HorizontalScroller)`
    display: flex;
    flex-direction: row;
    align-items: start;
    justify-content: start;
    margin-left: 5px;
    overflow: auto;

    & > div {
        &:not(:first-child) {
            border-left: 1px dashed;
            border-color: rgba(0, 0, 0, 0.3);
        }
    }
`;

const Column = styled.div`
    font-weight: bold;
    font-size: 12px;
    display: flex;
    flex-direction: column;
    justify-content: start;
    margin-right: 20px;

    &:not(:first-child) {
        padding-left: 20px;
    }
`;

const Heading = styled.div`
    display: flex;
    gap: 4px;
    align-items: center;
    margin-bottom: 4px;
`;

const Title = styled.div`
    font-size: 12px;
    font-weight: 600;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

interface Props {
    columns: SidebarStatsColumn[];
}

export const SidebarHeaderSectionColumns = ({ columns }: Props) => {
    if (!columns.length) return null;

    return (
        <ColumnsContainer scrollButtonSize={18} scrollButtonOffset={15}>
            {columns.map((column) => (
                <Column>
                    <Heading>
                        <Title>{column.title}</Title>
                    </Heading>
                    {column.content}
                </Column>
            ))}
        </ColumnsContainer>
    );
};
