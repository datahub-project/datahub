import React, { useEffect } from 'react';
import { Select, Typography } from 'antd';
import styled from 'styled-components/macro';
import { CorpGroup, EntityRelationship, EntityType } from '../../../../../types.generated';
import useGroupRelationships from '../../useGroupRelationships';
import { useEntityRegistry } from '../../../../useEntityRegistry';

const SelectGroupContainer = styled.div`
    margin-top: 32px;
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
    const { relationships } = useGroupRelationships();
    const entityRegistry = useEntityRegistry();

    const convertGroupRelationshipToOption = (relationship: EntityRelationship) => {
        const group: CorpGroup = relationship?.entity as CorpGroup;
        return {
            label: entityRegistry.getDisplayName(EntityType.CorpGroup, group),
            value: group?.urn,
        };
    };

    const options = relationships
        ?.filter((relationship) => !!relationship)
        .map((relationship) => convertGroupRelationshipToOption(relationship as EntityRelationship));

    useEffect(() => {
        if (!groupUrn && options && options.length === 1) {
            setGroupUrn?.(options[0].value);
        }
    }, [groupUrn, options, setGroupUrn]);

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
