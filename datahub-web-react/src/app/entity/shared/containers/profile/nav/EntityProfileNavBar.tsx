import { Affix } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useGetBrowsePathsQuery } from '../../../../../../graphql/browse.generated';
import { EntityType } from '../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useLineageData } from '../../../EntityContext';
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
    const lineage = useLineageData();

    return (
        <AffixWithHeight offsetTop={60}>
            <ProfileNavBrowsePath
                breadcrumbLinksEnabled={isBrowsable}
                type={entityType}
                path={browseData?.browsePaths?.[0]?.path || []}
                upstreams={lineage?.numUpstreamChildren || 0}
                downstreams={lineage?.numDownstreamChildren || 0}
            />
        </AffixWithHeight>
    );
};
