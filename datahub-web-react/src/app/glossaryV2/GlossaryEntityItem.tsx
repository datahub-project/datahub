import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import { Tooltip } from 'antd';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import { DisplayProperties, EntityType, GlossaryNode, GlossaryTerm } from '../../types.generated';
import { useEntityData } from '../entity/shared/EntityContext';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryNodeCard from './GlossaryNodeCard';
import GlossaryListCard from './GlossaryListCard';

interface Props {
    name: string;
    description?: string;
    urn: string;
    type: EntityType;
    descendants?: (GlossaryNode | GlossaryNodeFragment | GlossaryTerm | ChildGlossaryTermFragment)[];
    displayProperties?: Maybe<DisplayProperties>;
    showAsCard?: boolean; // Currently only supported for Nodes!
}

// iterates through the layers of descendents and counts the max number of layers
function countMaxDepth(
    descendents: (GlossaryNode | GlossaryNodeFragment | GlossaryTerm | ChildGlossaryTermFragment | undefined | null)[],
    depth = 1,
): number {
    let maxDepth = depth;

    descendents.forEach((desc) => {
        if (!desc) return;
        if (desc.type === EntityType.GlossaryNode) {
            const subNode = desc as unknown as GlossaryNodeFragment;
            if (subNode?.children?.relationships) {
                const subDepth = countMaxDepth(
                    subNode?.children?.relationships?.map(
                        (rel) => rel?.entity as GlossaryNodeFragment | ChildGlossaryTermFragment,
                    ),
                    depth + 1,
                );
                maxDepth = Math.max(maxDepth, subDepth);
            }
        }
    });

    return maxDepth;
}

function countTermsAndNodes(
    descendents: (GlossaryNode | GlossaryNodeFragment | GlossaryTerm | ChildGlossaryTermFragment)[],
): {
    termCount: number;
    nodeCount: number;
} {
    let termCount = 0;
    let nodeCount = 0; // Counting the root node itself

    function traverse(entity: GlossaryNodeFragment | ChildGlossaryTermFragment) {
        if (entity.type === EntityType.GlossaryTerm) {
            termCount++;
        } else if (entity.type === EntityType.GlossaryNode) {
            nodeCount++;
            const subNode = entity as unknown as GlossaryNodeFragment;
            if (subNode?.children?.relationships) {
                subNode.children.relationships.forEach((rel) =>
                    traverse(rel?.entity as GlossaryNodeFragment | ChildGlossaryTermFragment),
                );
            }
        }
    }

    descendents.forEach((desc) => traverse(desc));

    return { termCount, nodeCount };
}

function GlossaryEntityItem(props: Props) {
    const { name, description, urn, type, descendants, displayProperties } = props;
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    const { termCount, nodeCount } = countTermsAndNodes(descendants || []);
    const maxDepth = countMaxDepth(descendants || []);

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
