import React, { useState } from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import QueryCardHeader from './QueryCardHeader';
import QueryCardQuery from './QueryCardQuery';
import QueryCardDetails from './QueryCardDetails';

const Card = styled.div`
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 4px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    height: 380px;
    width: 32%;
    margin-right: 6px;
    margin-left: 6px;
    margin-bottom: 20px;
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
    onDeleted?: () => void;
    onClickExpand?: () => void;
    onClickEdit?: () => void;
    index?: number;
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
    onDeleted,
    onClickExpand,
    onClickEdit,
    index,
}: Props) {
    const [focused, setFocused] = useState(false);

    return (
        <Card onMouseEnter={() => setFocused(true)} onMouseLeave={() => setFocused(false)}>
            <QueryCardHeader query={query} focused={focused} onClickExpand={onClickExpand} />
            <QueryCardQuery query={query} showDetails={showDetails} onClickExpand={onClickExpand} index={index} />
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
