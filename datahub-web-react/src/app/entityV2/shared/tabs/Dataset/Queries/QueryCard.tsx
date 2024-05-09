import React, { useState } from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import QueryCardHeader from './QueryCardHeader';
import QueryCardQuery from './QueryCardQuery';
import QueryCardDetails from './QueryCardDetails';

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
