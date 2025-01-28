import React, { useState } from 'react';
import QueryModal from './QueryModal';
import QueryBuilderModal from './QueryBuilderModal';
import QueryCard from './QueryCard';

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
    onEdited?: (query) => void;
    index?: number;
    isCompact?: boolean;
};

export default function Query({
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
    onEdited,
    index,
    isCompact,
}: Props) {
    const [showQueryModal, setShowQueryModal] = useState(false);
    const [showEditQueryModal, setShowEditQueryModal] = useState(false);

    const onEditSubmitted = (newQuery) => {
        setShowEditQueryModal(false);
        onEdited?.(newQuery);
    };

    return (
        <React.Fragment key={index}>
            <QueryCard
                urn={urn}
                query={query}
                title={title}
                description={description}
                createdAtMs={createdAtMs}
                showEdit={showEdit}
                showDelete={showDelete}
                showDetails={showDetails}
                showHeader={showHeader}
                onClickEdit={() => setShowEditQueryModal(true)}
                onClickExpand={() => setShowQueryModal(true)}
                onDeleted={onDeleted}
                index={index}
                isCompact={isCompact}
            />
            {showQueryModal && (
                <QueryModal
                    query={query}
                    title={title}
                    description={description}
                    onClose={() => setShowQueryModal(false)}
                    showDetails={showDetails}
                />
            )}
            {showEditQueryModal && (
                <QueryBuilderModal
                    initialState={{ urn: urn as string, title, description, query }}
                    onSubmit={onEditSubmitted}
                    onClose={() => setShowEditQueryModal(false)}
                />
            )}
        </React.Fragment>
    );
}
