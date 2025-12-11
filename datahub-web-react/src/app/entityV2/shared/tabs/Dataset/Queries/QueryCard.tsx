/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import QueryCardDetails from '@app/entityV2/shared/tabs/Dataset/Queries/QueryCardDetails';
import QueryCardHeader from '@app/entityV2/shared/tabs/Dataset/Queries/QueryCardHeader';
import QueryCardQuery from '@app/entityV2/shared/tabs/Dataset/Queries/QueryCardQuery';

const Card = styled.div<{ isCompact?: boolean }>`
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 4px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    max-width: 450px;

    ${(props) => !props.isCompact && `height: 380px;`}
    ${(props) => props.isCompact && `max-width: 650px;`}
`;

export type Props = {
    urn?: string;
    query: string;
    title?: string;
    description?: string;
    createdAtMs?: number;
    showDelete?: boolean;
    showEdit?: boolean;
    showDetails?: boolean;
    showHeader?: boolean;
    onDeleted?: () => void;
    onClickExpand?: () => void;
    onClickEdit?: () => void;
    index?: number;
    isCompact?: boolean;
};

export default function QueryCard({
    urn,
    query,
    title,
    description,
    createdAtMs,
    showDelete,
    showEdit,
    showDetails = true,
    showHeader = true,
    onDeleted,
    onClickExpand,
    onClickEdit,
    index,
    isCompact,
}: Props) {
    const [focused, setFocused] = useState(false);

    return (
        <Card onMouseEnter={() => setFocused(true)} onMouseLeave={() => setFocused(false)} isCompact={isCompact}>
            {showHeader && <QueryCardHeader query={query} focused={focused} onClickExpand={onClickExpand} />}
            <QueryCardQuery
                query={query}
                showDetails={showDetails}
                onClickExpand={onClickExpand}
                index={index}
                isCompact={isCompact}
            />
            {showDetails && (
                <QueryCardDetails
                    urn={urn}
                    title={title}
                    description={description}
                    createdAtMs={createdAtMs}
                    showEdit={showEdit}
                    showDelete={showDelete}
                    onDeleted={onDeleted}
                    onClickEdit={onClickEdit}
                    onClickExpand={onClickExpand}
                    index={index}
                />
            )}
        </Card>
    );
}
