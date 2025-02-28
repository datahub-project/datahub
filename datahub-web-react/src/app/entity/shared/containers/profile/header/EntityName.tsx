import { message, Typography } from 'antd';
import React, { useState, useEffect } from 'react';
import styled from 'styled-components/macro';
import { useUpdateNameMutation } from '../../../../../../graphql/mutations.generated';
import { getParentNodeToUpdate, updateGlossarySidebar } from '../../../../../glossary/utils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData, useRefetch } from '../../../EntityContext';
import { useGlossaryEntityData } from '../../../GlossaryEntityContext';

export const EntityTitle = styled(Typography.Title)`
    &&& {
        margin-bottom: 0;
        word-break: break-all;
    }

    .ant-typography-edit {
        font-size: 16px;
        margin-left: 10px;
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
        updateName({ variables: { input: { name, urn } } })
            .then(() => {
                setUpdatedName(name);
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

    return (
        <>
            {isNameEditable ? (
                <EntityTitle
                    level={3}
                    disabled={isMutatingName}
                    editable={{
                        editing: isEditing,
                        onChange: handleChangeName,
                        onStart: handleStartEditing,
                    }}
                >
                    {updatedName}
                </EntityTitle>
            ) : (
                <EntityTitle level={3}>{entityName}</EntityTitle>
            )}
        </>
    );
}

export default EntityName;
