import { Affix } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { ProfileNavBrowsePath } from '@app/entity/shared/containers/profile/nav/ProfileNavBrowsePath';
import ProfileNavBrowsePathV2 from '@app/entity/shared/containers/profile/nav/ProfileNavBrowsePathV2';
import { useIsBrowseV2 } from '@app/search/useSearchAndBrowseVersion';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetBrowsePathsQuery } from '@graphql/browse.generated';
import { EntityType } from '@types';

type Props = {
    urn: string;
    entityType: EntityType;
};

const AffixWithHeight = styled(Affix)``;

export const EntityProfileNavBar = ({ urn, entityType }: Props) => {
    const showBrowseV2 = useIsBrowseV2();
    const { data: browseData } = useGetBrowsePathsQuery({
        variables: { input: { urn, type: entityType } },
        fetchPolicy: 'cache-first',
    });
    const entityRegistry = useEntityRegistry();
    const isBrowsable = entityRegistry.getBrowseEntityTypes().includes(entityType);

    return (
        <AffixWithHeight offsetTop={60}>
            {showBrowseV2 && <ProfileNavBrowsePathV2 urn={urn} type={entityType} />}
            {!showBrowseV2 && (
                <ProfileNavBrowsePath
                    urn={urn}
                    type={entityType}
                    breadcrumbLinksEnabled={isBrowsable}
                    path={browseData?.browsePaths?.[0]?.path || []}
                />
            )}
        </AffixWithHeight>
    );
};
