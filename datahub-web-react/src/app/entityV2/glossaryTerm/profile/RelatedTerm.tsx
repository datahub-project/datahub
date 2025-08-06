import { CloseOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { PreviewType } from '@app/entityV2/Entity';
import useRemoveRelatedTerms from '@app/entityV2/glossaryTerm/profile/useRemoveRelatedTerms';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetGlossaryTermQuery } from '@graphql/glossaryTerm.generated';
import { EntityType, TermRelationshipType } from '@types';

const TransparentButton = styled(Button)`
    color: ${(props) => props.theme.styles['primary-color']};
    font-size: 12px;
    box-shadow: none;
    border: none;
    padding: 0px 10px;
    position: absolute;
    top: 19px;
    right: 50px;
    display: none;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        color: ${(props) => props.theme.styles['primary-color']};
    }
`;

const ListItem = styled.div`
    position: relative;
    border: 1px solid #ebebeb;
    border-radius: 11px;

    &:hover ${TransparentButton} {
        display: inline-block;
    }
    &:hover {
        border: 1px solid ${(props) => props.theme.styles['primary-color']};
    }
`;

const Profile = styled.div`
    display: flex;
    position: relative;
    overflow: hidden;
    padding: 16px;
`;

interface Props {
    urn: string;
    relationshipType: TermRelationshipType;
    isEditable: boolean;
}

function RelatedTerm(props: Props) {
    const { urn, relationshipType, isEditable } = props;

    const entityRegistry = useEntityRegistry();
    const { data, loading } = useGetGlossaryTermQuery({ variables: { urn } });
    let displayName = '';
    if (data) {
        displayName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, data.glossaryTerm);
    }
    const { onRemove } = useRemoveRelatedTerms(urn, relationshipType, displayName);

    if (loading) return null;

    return (
        <ListItem>
            <Profile>
                {entityRegistry.renderPreview(EntityType.GlossaryTerm, PreviewType.PREVIEW, data?.glossaryTerm)}
                {isEditable && (
                    <TransparentButton size="small" onClick={onRemove}>
                        <CloseOutlined size={5} /> Remove Relationship
                    </TransparentButton>
                )}
            </Profile>
        </ListItem>
    );
}

export default RelatedTerm;
