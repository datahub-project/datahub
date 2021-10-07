import { Typography, Image, Button } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { capitalizeFirstLetter } from '../../../../../shared/capitalizeFirstLetter';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../../constants';
import { useEntityData } from '../../../EntityContext';
import { useEntityPath } from '../utils';

const PreviewImage = styled(Image)`
    max-height: 17px;
    width: auto;
    object-fit: contain;
    margin-right: 10px;
    background-color: transparent;
`;

const EntityTitle = styled(Typography.Title)`
    &&& {
        margin-bottom: 0;
    }
`;

const PlatformContent = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 8px;
`;

const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 700;
    color: ${ANTD_GRAY[7]};
`;

const PlatformDivider = styled.div`
    display: inline-block;
    padding-left: 10px;
    margin-right: 10px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    height: 18px;
    vertical-align: text-top;
`;

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: space-between;
    margin-bottom: 4px;
`;

const MainHeaderContent = styled.div`
    flex: 1;
`;

export const EntityHeader = () => {
    const { urn, entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const platformName = capitalizeFirstLetter(entityData?.platform?.name);
    const platformLogoUrl = entityData?.platform?.info?.logoUrl;
    const entityTypeCased = entityRegistry.getEntityName(entityType);
    const entityPath = useEntityPath(entityType, urn);
    const externalUrl = entityData?.externalUrl || undefined;
    const hasExternalUrl = !!externalUrl;
    return (
        <HeaderContainer>
            <MainHeaderContent>
                <PlatformContent>
                    <span>
                        {!!platformLogoUrl && <PreviewImage preview={false} src={platformLogoUrl} alt={platformName} />}
                    </span>
                    <PlatformText>{platformName}</PlatformText>
                    <PlatformDivider />
                    <PlatformText>{entityData?.entityTypeOverride || entityTypeCased}</PlatformText>
                </PlatformContent>
                <Link to={entityPath}>
                    <EntityTitle level={3}>{entityData?.name || ' '}</EntityTitle>
                </Link>
            </MainHeaderContent>
            {hasExternalUrl && <Button href={externalUrl}>View in {platformName}</Button>}
        </HeaderContainer>
    );
};
