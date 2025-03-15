import GenericEntityPage, { GENERIC_ENTITY_PAGE_PATH } from '@app/GenericEntityPage';
import TypedEntityPage from '@app/TypedEntityPage';
import { EntityType } from '@types';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Switch, Route } from 'react-router-dom';
import { PageRoutes } from '../../conf/Global';
import { useEntityRegistry } from '../useEntityRegistry';
import ManageDomainsPageV2 from './nestedDomains/ManageDomainsPageV2';
import ManageDomainsSidebar from './nestedDomains/ManageDomainsSidebar';
import { DomainsContext } from './DomainsContext';
import { GenericEntityProperties } from '../entity/shared/types';

const ContentWrapper = styled.div`
    display: flex;
    flex: 1;
    overflow: hidden;
`;

export default function DomainRoutes() {
    const entityRegistry = useEntityRegistry();
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [parentDomainsToUpdate, setParentDomainsToUpdate] = useState<string[]>([]);

    return (
        <DomainsContext.Provider value={{ entityData, setEntityData, parentDomainsToUpdate, setParentDomainsToUpdate }}>
            <ContentWrapper>
                <ManageDomainsSidebar />
                <Switch>
                    <Route
                        path={`${PageRoutes.DOMAIN}${GENERIC_ENTITY_PAGE_PATH}`}
                        render={() => <GenericEntityPage />}
                    />
                    <Route
                        path={`/${entityRegistry.getPathName(EntityType.Domain)}/:urn/:tab?`}
                        render={() => <TypedEntityPage entityType={EntityType.Domain} />}
                    />
                    <Route path={PageRoutes.DOMAINS} render={() => <ManageDomainsPageV2 />} />
                </Switch>
            </ContentWrapper>
        </DomainsContext.Provider>
    );
}
