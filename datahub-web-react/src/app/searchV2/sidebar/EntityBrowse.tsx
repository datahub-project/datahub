import React from 'react';
import EntityNode from './EntityNode';
import { BrowseProvider } from './BrowseContext';
import SidebarLoadingError from './SidebarLoadingError';
import useSidebarEntities from './useSidebarEntities';

type Props = {
    visible: boolean;
};

const EntityBrowse = ({ visible }: Props) => {
    const { error, entityAggregations, retry } = useSidebarEntities({
        skip: !visible,
    });

    return (
        <>
            {entityAggregations && !entityAggregations.length && <div>No results found</div>}
            {entityAggregations?.map((entityAggregation) => (
                <BrowseProvider key={entityAggregation.value} entityAggregation={entityAggregation}>
                    <EntityNode />
                </BrowseProvider>
            ))}
            {error && <SidebarLoadingError onClickRetry={retry} />}
        </>
    );
};

export default EntityBrowse;
