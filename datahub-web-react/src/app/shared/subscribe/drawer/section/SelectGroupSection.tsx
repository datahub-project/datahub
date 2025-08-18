import { Button, Text } from '@components';
import { Select, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import CreateGroupModal from '@app/identity/group/CreateGroupModal';
import Loading from '@app/shared/Loading';
import { getGroupOptions } from '@app/shared/subscribe/drawer/section/SelectGroupSection.utils';
import useGroupRelationships from '@app/shared/subscribe/useGroupRelationships';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpGroup, EntityRelationshipsResult } from '@types';

const SelectGroupContainer = styled.div`
    margin-top: 32px;
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const TitleText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
`;

const GroupSelect = styled(Select)`
    width: 100%;
`;

const NewGroupButton = styled(Button)`
    align-self: flex-end;
`;

interface Props {
    groupUrn?: string;
    setGroupUrn?: (groupUrn: string) => void;
}

export default function SelectGroupSection({ groupUrn, setGroupUrn }: Props) {
    const { relationships, ownedGroupSearchResults, refetch } = useGroupRelationships();
    const entityRegistry = useEntityRegistry();

    const [isCreateGroupModalOpen, setIsCreateGroupModalOpen] = useState(false);
    const [isRefetching, setIsRefetching] = useState(false);

    const options = getGroupOptions(
        relationships as EntityRelationshipsResult['relationships'],
        ownedGroupSearchResults,
        entityRegistry,
    );

    useEffect(() => {
        if (!groupUrn && options.length === 1) {
            setGroupUrn?.(options[0].value);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [groupUrn, options.length, setGroupUrn]);

    return (
        <>
            <SelectGroupContainer>
                <TitleText>Group to notify</TitleText>
                {/* ----------------------------- Select group dropdown ----------------------------- */}
                <GroupSelect
                    data-testid="select-group-dropdown"
                    placeholder="Select a group"
                    options={options}
                    value={groupUrn}
                    onSelect={(value) => {
                        setGroupUrn?.(value as string);
                    }}
                />
                {/* ----------------------------- Option to create a group instead ----------------------------- */}
                <NewGroupButton variant="link" onClick={() => setIsCreateGroupModalOpen(true)} disabled={isRefetching}>
                    {isRefetching ? (
                        [<Loading marginTop={0} height={12} />, <Text>Creating...</Text>]
                    ) : (
                        <Text>Create Group</Text>
                    )}
                </NewGroupButton>
            </SelectGroupContainer>

            {isCreateGroupModalOpen && (
                <CreateGroupModal
                    onClose={() => setIsCreateGroupModalOpen(false)}
                    onCreate={(group: CorpGroup) => {
                        setIsCreateGroupModalOpen(false);
                        setIsRefetching(true);
                        setTimeout(() => {
                            refetch();
                            setIsRefetching(false);
                            setGroupUrn?.(group.urn);
                        }, 3000);
                    }}
                />
            )}
        </>
    );
}
