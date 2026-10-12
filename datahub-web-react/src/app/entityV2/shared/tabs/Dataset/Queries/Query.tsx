import React, { useState } from 'react';

import QueryCard from '@app/entityV2/shared/tabs/Dataset/Queries/QueryCard';
import QueryModal from '@app/entityV2/shared/tabs/Dataset/Queries/QueryModal';

type Props = {
    query: string;
    title?: string;
    description?: string;
    createdAtMs?: number;
    showDetails?: boolean;
    showHeader?: boolean;
    index?: number;
    isCompact?: boolean;
};

export default function Query({
    query,
    title,
    description,
    createdAtMs,
    showDetails = true,
    showHeader = true,
    index,
    isCompact,
}: Props) {
    const [showQueryModal, setShowQueryModal] = useState(false);

    return (
        <React.Fragment key={index}>
            <QueryCard
                query={query}
                title={title}
                description={description}
                createdAtMs={createdAtMs}
                showDetails={showDetails}
                showHeader={showHeader}
                onClickExpand={() => setShowQueryModal(true)}
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
        </React.Fragment>
    );
}
