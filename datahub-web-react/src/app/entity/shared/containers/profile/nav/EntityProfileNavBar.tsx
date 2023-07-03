import React from 'react';
import styled from 'styled-components/macro';
import { Affix } from 'antd';
import { useGetBrowsePathsQuery } from '../../../../../../graphql/browse.generated';
import { EntityType } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { ProfileNavBrowsePath } from './ProfileNavBrowsePath';

type Props = {
    urn: string;
    entityType: EntityType;
};

const AffixWithHeight = styled(Affix)``;

export const EntityProfileNavBar = ({ urn, entityType }: Props) => {
    const { data: browseData } = useGetBrowsePathsQuery({
        variables: { input: { urn, type: entityType } },
        fetchPolicy: 'cache-first',
    });
    const entityRegistry = useEntityRegistry();
    const isBrowsable = entityRegistry.getBrowseEntityTypes().includes(entityType);

    return (
        <AffixWithHeight offsetTop={60}>
            <ProfileNavBrowsePath
                urn={urn}
                type={entityType}
                breadcrumbLinksEnabled={isBrowsable}
                path={browseData?.browsePaths?.[0]?.path || []}
            />
        </AffixWithHeight>
    );
};
