import React from 'react';
import styled from 'styled-components/macro';
import { Affix } from 'antd';
import { useGetBrowsePathsQuery } from '../../../../../../graphql/browse.generated';
import { EntityType } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { ProfileNavBrowsePath } from './ProfileNavBrowsePath';
import { useAppConfig } from '../../../../../useAppConfig';
import ProfileNavBrowsePathV2 from './ProfileNavBrowsePathV2';

type Props = {
    urn: string;
    entityType: EntityType;
};

const AffixWithHeight = styled(Affix)``;

export const EntityProfileNavBar = ({ urn, entityType }: Props) => {
    const appConfig = useAppConfig();
    const { data: browseData } = useGetBrowsePathsQuery({
        variables: { input: { urn, type: entityType } },
        fetchPolicy: 'cache-first',
    });
    const entityRegistry = useEntityRegistry();
    const isBrowsable = entityRegistry.getBrowseEntityTypes().includes(entityType);

    const { showBrowseV2 } = appConfig.config.featureFlags;

    return (
        <AffixWithHeight offsetTop={60}>
            {showBrowseV2 && <ProfileNavBrowsePathV2 />}
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
