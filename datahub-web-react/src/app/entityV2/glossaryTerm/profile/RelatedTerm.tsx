import React from 'react';
import { CloseOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import styled from 'styled-components/macro';
import { useGetGlossaryTermQuery } from '../../../../graphql/glossaryTerm.generated';
import { EntityType, TermRelationshipType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';
import useRemoveRelatedTerms from './useRemoveRelatedTerms';
import { REDESIGN_COLORS } from '../../shared/constants';

const TransparentButton = styled(Button)`
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
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
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
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
        border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};
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
