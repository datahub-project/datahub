import React from 'react';
import { Select, Typography } from 'antd';
import styled from 'styled-components/macro';
import { useGetUserGroupsQuery } from '../../../../../graphql/user.generated';
import { useUserContext } from '../../../../context/useUserContext';
import { CorpGroup, EntityRelationship } from '../../../../../types.generated';
import { getGroupName } from '../../../../settings/personal/utils';

const SelectGroupContainer = styled.div`
    margin-top: 32px;
    margin-left: 8px;
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const TitleText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
`;

const GroupSelect = styled(Select)`
    width: 100%;
`;

interface Props {
    groupUrn?: string;
    setGroupUrn?: (groupUrn: string) => void;
}

export default function SelectGroupSection({ groupUrn, setGroupUrn }: Props) {
    const authenticatedUserUrn = useUserContext()?.user?.urn;
    const { data: groupsData } = useGetUserGroupsQuery({
        skip: !authenticatedUserUrn,
        variables: { urn: authenticatedUserUrn as string, start: 0, count: 100 },
    });

    const convertGroupRelationshipToOption = (relationship: EntityRelationship) => {
        const group: CorpGroup = relationship?.entity as CorpGroup;
        return {
            label: getGroupName(group),
            value: group?.urn,
        };
    };

    const options = groupsData?.corpUser?.relationships?.relationships
        .filter((relationship) => !!relationship)
        .map((relationship) => convertGroupRelationshipToOption(relationship as EntityRelationship));

    return (
        <>
            <SelectGroupContainer>
                <TitleText>Group to notify</TitleText>
                <GroupSelect
                    placeholder="Select a group"
                    options={options}
                    value={groupUrn}
                    onSelect={(value) => {
                        setGroupUrn?.(value as string);
                    }}
                />
            </SelectGroupContainer>
        </>
    );
}
