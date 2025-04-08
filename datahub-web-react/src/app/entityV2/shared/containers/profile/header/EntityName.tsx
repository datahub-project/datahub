import { Typography, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { getParentNodeToUpdate, updateGlossarySidebar } from '@app/glossary/utils';
import CompactContext from '@app/shared/CompactContext';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useEmbeddedProfileLinkProps } from '@src/app/shared/useEmbeddedProfileLinkProps';

import { useUpdateNameMutation } from '@graphql/mutations.generated';
import { EntityType } from '@types';

const EntityTitle = styled(Typography.Text)<{ $showEntityLink?: boolean }>`
    font-weight: 700;
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
    line-height: normal;

    ${(props) =>
        props.$showEntityLink &&
        `
    :hover {
        color: ${REDESIGN_COLORS.HOVER_PURPLE};
    }
    `}
    &&& {
        margin-bottom: 0;
        word-break: break-word;
        overflow: hidden;
        text-overflow: ellipsis;
    }

    .ant-typography-edit {
        font-size: 12px;
        margin-left: 2px;

        & svg {
            fill: #533fd1;
        }
    }
`;

interface Props {
    isNameEditable?: boolean;
}

function EntityName(props: Props) {
    const { isNameEditable } = props;
    const refetch = useRefetch();
    const entityRegistry = useEntityRegistry();
    const { isInGlossaryContext, urnsToUpdate, setUrnsToUpdate } = useGlossaryEntityData();
    const { urn, entityType, entityData } = useEntityData();
    const entityName = entityData ? entityRegistry.getDisplayName(entityType, entityData) : '';
    const [updatedName, setUpdatedName] = useState(entityName);
    const [isEditing, setIsEditing] = useState(false);

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
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to update name: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const Title = isNameEditable ? (
        <EntityTitle
            disabled={isMutatingName}
            editable={{
                editing: isEditing,
                onChange: handleChangeName,
                onStart: handleStartEditing,
            }}
            $showEntityLink={showEntityLink}
        >
            {updatedName}
        </EntityTitle>
    ) : (
        <EntityTitle $showEntityLink={showEntityLink}>{entityName}</EntityTitle>
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
