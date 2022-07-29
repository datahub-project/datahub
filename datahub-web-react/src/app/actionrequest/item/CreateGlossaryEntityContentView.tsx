import { Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { ActionRequest, EntityType, GlossaryNode } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import CreatedByView from './CreatedByView';

const NameWrapper = styled.span`
    font-weight: bold;
`;

interface Props {
    proposedName: string;
    actionRequest: ActionRequest;
    entityName: string;
    parentNode: GlossaryNode | null;
}

function CreateGlossaryEntityContentView({ proposedName, actionRequest, entityName, parentNode }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <span>
            <CreatedByView actionRequest={actionRequest} />
            <Typography.Text>
                {' '}
                requests to create {entityName} <NameWrapper>{proposedName}</NameWrapper>
                {parentNode && (
                    <Typography.Text>
                        {' '}
                        under parent&nbsp;
                        <Link to={`/${entityRegistry.getPathName(EntityType.GlossaryNode)}/${parentNode.urn}`}>
                            <Typography.Text strong>
                                {entityRegistry.getDisplayName(EntityType.GlossaryNode, parentNode)}
                            </Typography.Text>
                        </Link>
                    </Typography.Text>
                )}
                {!parentNode && <Typography.Text> at the root level</Typography.Text>}
            </Typography.Text>
        </span>
    );
}

export default CreateGlossaryEntityContentView;
