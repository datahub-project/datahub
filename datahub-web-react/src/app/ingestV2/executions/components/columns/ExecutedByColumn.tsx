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

import UserWithAvatar from '@app/ingestV2/shared/components/UserWithAvatar';
import { CLI_INGESTION_SOURCE, MANUAL_INGESTION_SOURCE, SCHEDULED_INGESTION_SOURCE } from '@app/ingestV2/source/utils';

import { CorpUser } from '@types';

const UserContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    wrap: auto;
`;

interface SourceColumnProps {
    source?: string | null;
    actor?: CorpUser | null;
}

export function ExecutedByColumn({ source, actor }: SourceColumnProps) {
    switch (source) {
        case MANUAL_INGESTION_SOURCE:
            if (!actor) return <>Manual Execution</>;
            return (
                <UserContainer>
                    Manual Execution by <UserWithAvatar user={actor} />
                </UserContainer>
            );

        case SCHEDULED_INGESTION_SOURCE:
            return <span>Scheduled Execution</span>;

        case CLI_INGESTION_SOURCE:
            return <span>CLI Execution</span>;

        default:
            return <span>-</span>;
    }
}
