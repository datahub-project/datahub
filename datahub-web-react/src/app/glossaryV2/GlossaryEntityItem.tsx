import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { Tooltip } from 'antd';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import { DisplayProperties, EntityType, GlossaryNode, GlossaryTerm } from '../../types.generated';
import { useEntityData } from '../entity/shared/EntityContext';
import { GenericEntityProperties } from '../entity/shared/types';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryNodeCard from './GlossaryNodeCard';
import GlossaryTermItem from './GlossaryTermItem';

const GlossaryItem = styled.div`
    align-items: center;
    color: ${REDESIGN_COLORS.SUBTITLE};
    font-size: 14px;
    font-weight: 400;
    line-height: normal;
    width: 100%;
    height: 100%;
    position: relative;
    overflow: hidden;
    padding: 8px;

    .anticon-folder {
        margin-right: 8px;
    }
`;

interface ItemWrapperProps {
    type: EntityType;
    entityData: {
        urn: string;
        entityType: EntityType;
        entityData: GenericEntityProperties | null;
        loading: boolean;
    };
}

const ItemWrapper = styled.div<ItemWrapperProps>`
    transition: 0.15s;
    width: 100%;
    flex-basis: ${(props) => (props.type === EntityType.GlossaryNode && !props.entityData.urn ? '24%' : 'auto')};
    min-width: 200px;
    & a {
        display: block;
        width: 100%;
        height: 100%;
    }
    &:hover {
        transition: 0.15s;
        background-color: ${REDESIGN_COLORS.LIGHT_GREY};
        border-radius: 16px;
    }
`;

interface Props {
    name: string;
    description?: string;
    urn: string;
    type: EntityType;
    descendants?: (GlossaryNode | GlossaryNodeFragment | GlossaryTerm | ChildGlossaryTermFragment)[];
    displayProperties?: Maybe<DisplayProperties>;
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
            <ItemWrapper type={type} entityData={entityData}>
                <Link to={`${entityRegistry.getEntityUrl(type, urn)}`}>
                    <GlossaryItem>
                        {type === EntityType.GlossaryNode && !entityData.urn ? (
                            <GlossaryNodeCard
                                name={name}
                                type={type}
                                description={description}
                                displayProperties={displayProperties}
                                urn={urn}
                                termCount={termCount}
                                nodeCount={nodeCount}
                                maxDepth={maxDepth}
                            />
                        ) : (
                            <GlossaryTermItem
                                name={name}
                                description={description}
                                type={type}
                                urn={urn}
                                entityData={entityData}
                            />
                        )}
                    </GlossaryItem>
                </Link>
            </ItemWrapper>
        </Tooltip>
    );
}

export default GlossaryEntityItem;
