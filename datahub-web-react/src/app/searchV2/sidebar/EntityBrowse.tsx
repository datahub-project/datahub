import React from 'react';
import { useTranslation } from 'react-i18next';

import { BrowseProvider } from '@app/searchV2/sidebar/BrowseContext';
import EntityNode from '@app/searchV2/sidebar/EntityNode';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useSidebarEntities from '@app/searchV2/sidebar/useSidebarEntities';

type Props = {
    visible: boolean;
};

const EntityBrowse = ({ visible }: Props) => {
    const { t: tc } = useTranslation('common.actions');
    const { error, entityAggregations, retry } = useSidebarEntities({
        skip: !visible,
    });

    return (
        <>
            {entityAggregations && !entityAggregations.length && <div>{tc('noResults')}</div>}
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
