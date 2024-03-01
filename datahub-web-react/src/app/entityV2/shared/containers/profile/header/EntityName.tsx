import { message, Typography } from 'antd';
import React, { useState, useEffect } from 'react';
import styled from 'styled-components/macro';
import { useUpdateNameMutation } from '../../../../../../graphql/mutations.generated';
import { getParentNodeToUpdate, updateGlossarySidebar } from '../../../../../glossary/utils';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useEntityData, useRefetch } from '../../../EntityContext';
import { useGlossaryEntityData } from '../../../GlossaryEntityContext';

const EntityTitle = styled(Typography.Text)`
    font-size: 16px;
    font-weight: 600;
    color: #533fd1;
    line-height: normal;
    margin-right: 10px;

    &&& {
        margin-bottom: 0;
        word-break: break-all;
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

    return (
        <>
            {isNameEditable ? (
                <EntityTitle
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
                <EntityTitle>{entityName}</EntityTitle>
            )}
        </>
    );
}

export default EntityName;
