import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Divider, Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { useGetGlossaryTermQuery } from '../../../../graphql/glossaryTerm.generated';
import { EntityType, TermRelationshipType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';
import useRemoveRelatedTerms from './useRemoveRelatedTerms';

const ListItem = styled.div`
    margin: 0 20px;
`;

const Profile = styled.div`
    display: felx;
    marging-bottom: 20px;
`;

const MenuIcon = styled(MoreOutlined)`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: 20px;
    height: 32px;
    margin-left: -10px;
`;

const MenuItem = styled.div`
    font-size: 12px;
    padding: 0 4px;
    color: #262626;
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

    const items = [
        {
            key: '0',
            label: (
                <MenuItem onClick={onRemove}>
                    <DeleteOutlined /> &nbsp; Remove Term
                </MenuItem>
            ),
        },
    ];

    return (
        <ListItem>
            <Profile>
                {entityRegistry.renderPreview(EntityType.GlossaryTerm, PreviewType.PREVIEW, data?.glossaryTerm)}
                {isEditable && (
                    <Dropdown menu={{ items }} trigger={['click']}>
                        <MenuIcon />
                    </Dropdown>
                )}
            </Profile>
            <Divider style={{ margin: '20px 0' }} />
        </ListItem>
    );
}

export default RelatedTerm;
