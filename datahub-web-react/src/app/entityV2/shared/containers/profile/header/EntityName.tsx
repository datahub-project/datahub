import { Typography, message } from 'antd';
import React, { useContext, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import colors from '@components/theme/foundations/colors';

import { useDomainsContext } from '@app/domainV2/DomainsContext';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { getParentNodeToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import { getReloadableModuleKey } from '@app/homeV3/modules/utils';
import CompactContext from '@app/shared/CompactContext';
import usePrevious from '@app/shared/usePrevious';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { getColor } from '@src/alchemy-components/theme/utils';
import { useEmbeddedProfileLinkProps } from '@src/app/shared/useEmbeddedProfileLinkProps';

import { useUpdateNameMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, EntityType } from '@types';

const EntityTitle = styled(Typography.Text)<{ $showEntityLink?: boolean }>`
    font-weight: 700;
    color: ${(p) => p.theme.styles['primary-color']};
    line-height: normal;

    ${(props) =>
        props.$showEntityLink &&
        `
    :hover {
        color: ${(p) => getColor('primary', 600, p.theme)};
    }
    `}
    &&& {
        margin-bottom: 0;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }

    .ant-typography-edit {
        font-size: 12px;
        margin-left: 2px;

        & svg {
            fill: ${(p) => p.theme.styles['primary-color']};
        }
    }
`;

interface Props {
    isNameEditable?: boolean;
}

function EntityName(props: Props) {
    const { isNameEditable } = props;
    const { isClosed: isSidebarClosed } = useContext(EntitySidebarContext);
    const refetch = useRefetch();
    const entityRegistry = useEntityRegistry();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate } = useGlossaryEntityData();
    const { setUpdatedDomain } = useDomainsContext();
    const { urn, entityType, entityData } = useEntityData();
    const entityName = entityData ? entityRegistry.getDisplayName(entityType, entityData) : '';
    const [updatedName, setUpdatedName] = useState(entityName);
    const [isEditing, setIsEditing] = useState(false);
    const { reloadByKeyType } = useReloadableContext();

    const isCompact = React.useContext(CompactContext);
    const showEntityLink = isCompact && entityType !== EntityType.Query;

    useEffect(() => {
        setUpdatedName(entityName);
    }, [entityName]);

    const [updateName, { loading: isMutatingName }] = useUpdateNameMutation();

    const handleStartEditing = () => {
        setIsEditing(true);
    };

    const handleChangeName = (name: string) => {
        if (name === entityName) {
            setIsEditing(false);
            return;
        }
        setUpdatedName(name);
        updateName({ variables: { input: { name, urn } } })
            .then(() => {
                setIsEditing(false);
                message.success({ content: 'Name Updated', duration: 2 });
                refetch();
                if (isInGlossaryContext) {
                    const parentNodeToUpdate = getParentNodeToUpdate(entityData, entityType);
                    updateGlossarySidebar([parentNodeToUpdate], urnsToUpdate, setUrnsToUpdate);
                }
                if (setUpdatedDomain !== undefined) {
                    const updatedDomain = {
                        urn,
                        type: EntityType.Domain,
                        id: urn,
                        properties: {
                            name,
                        },
                    };
                    setUpdatedDomain(updatedDomain);
                }
                // Reload modules as name of some asset could be changed in them
                reloadByKeyType(
                    [
                        getReloadableModuleKey(DataHubPageModuleType.AssetCollection),
                        getReloadableModuleKey(DataHubPageModuleType.OwnedAssets),
                        getReloadableModuleKey(DataHubPageModuleType.Assets),
                        getReloadableModuleKey(DataHubPageModuleType.ChildHierarchy),
                        getReloadableModuleKey(DataHubPageModuleType.Domains),
                    ],
                    3000,
                );
                if (entityType === EntityType.GlossaryTerm) {
                    reloadByKeyType([getReloadableModuleKey(DataHubPageModuleType.RelatedTerms)], 3000);
                }
                if (entityType === EntityType.DataProduct) {
                    reloadByKeyType([getReloadableModuleKey(DataHubPageModuleType.DataProducts)], 3000);
                }
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to update name: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    // The following handles bug where sidebar opening causes ellipses and closing it doesn't show full name again
    const [key, setKey] = useState(0);

    const previousIsSidebarClosed = usePrevious(isSidebarClosed);
    useEffect(() => {
        if (isSidebarClosed !== previousIsSidebarClosed) {
            // timeout to wait for sidebar to fully re-open
            setTimeout(() => {
                setKey((prev) => prev + 1);
            }, 200);
        }
    }, [isSidebarClosed, previousIsSidebarClosed]);

    const Title = isNameEditable ? (
        <EntityTitle
            disabled={isMutatingName}
            editable={{
                editing: isEditing,
                onChange: handleChangeName,
                onStart: handleStartEditing,
            }}
            $showEntityLink={showEntityLink}
            ellipsis={{
                tooltip: { showArrow: false, color: 'white', overlayInnerStyle: { color: colors.gray[1700] } },
            }}
            key={`${updatedName}-${key}`}
        >
            {updatedName}
        </EntityTitle>
    ) : (
        <EntityTitle
            $showEntityLink={showEntityLink}
            ellipsis={{
                tooltip: { showArrow: false, color: 'white', overlayInnerStyle: { color: colors.gray[1700] } },
            }}
            key={`${entityName}-${key}`}
        >
            {entityName}
        </EntityTitle>
    );

    // have entity link open new tab if in the chrome extension
    const linkProps = useEmbeddedProfileLinkProps();

    return showEntityLink ? (
        <Link to={`${entityRegistry.getEntityUrl(entityType, urn)}/`} {...linkProps}>
            {Title}
        </Link>
    ) : (
        Title
    );
}

export default EntityName;
