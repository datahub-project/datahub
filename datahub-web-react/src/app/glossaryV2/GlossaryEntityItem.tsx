import { Tooltip } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';

import { useEntityData } from '@app/entity/shared/EntityContext';
import GlossaryListCard from '@app/glossaryV2/GlossaryListCard';
import GlossaryNodeCard from '@app/glossaryV2/GlossaryNodeCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GlossaryNodeFragment, RootGlossaryNodeWithFourLayersFragment } from '@graphql/fragments.generated';
import { DisplayProperties, EntityType, GlossaryNode } from '@types';

interface Props {
    name: string;
    description?: string;
    urn: string;
    type: EntityType;
    displayProperties?: Maybe<DisplayProperties>;
    showAsCard?: boolean; // Currently only supported for Nodes!
    node?: GlossaryNode | GlossaryNodeFragment | RootGlossaryNodeWithFourLayersFragment;
}

// iterates through the layers of descendents and counts the max number of layers
function countMaxDepth(initialNode?: RootGlossaryNodeWithFourLayersFragment, depth = 1): number {
    let maxDepth = depth;

    initialNode?.glossaryChildrenSearch?.searchResults
        .map((r) => r.entity as RootGlossaryNodeWithFourLayersFragment)
        .forEach((subNode) => {
            if (subNode.type === EntityType.GlossaryNode) {
                if (subNode?.glossaryChildrenSearch?.searchResults) {
                    const subDepth = countMaxDepth(subNode, depth + 1);
                    maxDepth = Math.max(maxDepth, subDepth);
                }
            }
        });

    return maxDepth;
}

function countTermsAndNodes(initialNode?: RootGlossaryNodeWithFourLayersFragment): {
    termCount: number;
    nodeCount: number;
} {
    let termCount = initialNode?.childrenCount?.termsCount || 0;
    let nodeCount = initialNode?.childrenCount?.nodesCount || 0;

    function traverse(childNode: RootGlossaryNodeWithFourLayersFragment) {
        nodeCount += childNode.childrenCount?.nodesCount || 0;
        termCount += childNode.childrenCount?.termsCount || 0;
        childNode.glossaryChildrenSearch?.searchResults
            .map((r) => r.entity as RootGlossaryNodeWithFourLayersFragment)
            .forEach((n) => traverse(n));
    }

    initialNode?.glossaryChildrenSearch?.searchResults
        .map((r) => r.entity as RootGlossaryNodeWithFourLayersFragment)
        .forEach((n) => traverse(n));

    return { termCount, nodeCount };
}

function GlossaryEntityItem(props: Props) {
    const { name, description, urn, type, displayProperties, node } = props;
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    const { termCount, nodeCount } = countTermsAndNodes(node as RootGlossaryNodeWithFourLayersFragment);
    const maxDepth = countMaxDepth(node as RootGlossaryNodeWithFourLayersFragment);

    return (
        <Tooltip title={name} showArrow={false} placement="top">
            <Link to={`${entityRegistry.getEntityUrl(type, urn)}`}>
                {type === EntityType.GlossaryNode && props.showAsCard ? (
                    <GlossaryNodeCard
                        name={name}
                        description={description}
                        displayProperties={displayProperties}
                        urn={urn}
                        termCount={termCount}
                        nodeCount={nodeCount}
                        maxDepth={maxDepth}
                    />
                ) : (
                    <GlossaryListCard
                        name={name}
                        description={description}
                        type={type}
                        urn={urn}
                        entityData={entityData}
                        termCount={termCount}
                        nodeCount={nodeCount}
                    />
                )}
            </Link>
        </Tooltip>
    );
}

export default GlossaryEntityItem;
