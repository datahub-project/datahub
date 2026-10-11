import React, { useState } from 'react';
import styled from 'styled-components';

import QueryCardDetails from '@app/entityV2/shared/tabs/Dataset/Queries/QueryCardDetails';
import QueryCardHeader from '@app/entityV2/shared/tabs/Dataset/Queries/QueryCardHeader';
import QueryCardQuery from '@app/entityV2/shared/tabs/Dataset/Queries/QueryCardQuery';

const Card = styled.div<{ isCompact?: boolean }>`
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 4px;
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    max-width: 450px;

    ${(props) => !props.isCompact && `height: 380px;`}
    ${(props) => props.isCompact && `max-width: 650px;`}
`;

type Props = {
    query: string;
    title?: string;
    description?: string;
    createdAtMs?: number;
    showDetails?: boolean;
    showHeader?: boolean;
    onClickExpand?: () => void;
    index?: number;
    isCompact?: boolean;
};

export default function QueryCard({
    query,
    title,
    description,
    createdAtMs,
    showDetails = true,
    showHeader = true,
    onClickExpand,
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
                    title={title}
                    description={description}
                    createdAtMs={createdAtMs}
                    onClickExpand={onClickExpand}
                />
            )}
        </Card>
    );
}
