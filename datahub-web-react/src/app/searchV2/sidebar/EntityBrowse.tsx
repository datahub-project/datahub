/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { BrowseProvider } from '@app/searchV2/sidebar/BrowseContext';
import EntityNode from '@app/searchV2/sidebar/EntityNode';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useSidebarEntities from '@app/searchV2/sidebar/useSidebarEntities';

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
