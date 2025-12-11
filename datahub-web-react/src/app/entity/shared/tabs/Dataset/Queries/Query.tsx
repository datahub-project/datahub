/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useState } from 'react';

import QueryBuilderModal from '@app/entity/shared/tabs/Dataset/Queries/QueryBuilderModal';
import QueryCard from '@app/entity/shared/tabs/Dataset/Queries/QueryCard';
import QueryModal from '@app/entity/shared/tabs/Dataset/Queries/QueryModal';

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
    onEdited?: (query) => void;
    index?: number;
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
    onDeleted,
    onEdited,
    index,
}: Props) {
    const [showQueryModal, setShowQueryModal] = useState(false);
    const [showEditQueryModal, setShowEditQueryModal] = useState(false);

    const onEditSubmitted = (newQuery) => {
        setShowEditQueryModal(false);
        onEdited?.(newQuery);
    };

    return (
        <>
            <QueryCard
                urn={urn}
                query={query}
                title={title}
                description={description}
                createdAtMs={createdAtMs}
                showEdit={showEdit}
                showDelete={showDelete}
                showDetails={showDetails}
                onClickEdit={() => setShowEditQueryModal(true)}
                onClickExpand={() => setShowQueryModal(true)}
                onDeleted={onDeleted}
                index={index}
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
        </>
    );
}
