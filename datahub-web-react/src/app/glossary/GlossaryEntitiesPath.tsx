import { Breadcrumb } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { PageRoutes } from '../../conf/Global';
import { GlossaryNode, GlossaryTerm, ParentNodesResult } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { BreadcrumbItem } from '../entity/shared/containers/profile/nav/ProfileNavBrowsePath';
import { useGlossaryEntityData } from '../entity/shared/GlossaryEntityContext';
import { useEntityRegistry } from '../useEntityRegistry';

const PathWrapper = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    display: flex;
    padding: 10px 24px;
`;

const BreadcrumbsWrapper = styled(Breadcrumb)`
    font-size: 16px;
`;

function getGlossaryBreadcrumbs(parentNodes: Maybe<ParentNodesResult>, entity: Maybe<GlossaryNode | GlossaryTerm>) {
    const nodes = parentNodes?.nodes;
    const breadcrumbs: (GlossaryTerm | GlossaryNode)[] = [...(nodes || [])].reverse();
    if (entity) breadcrumbs.push(entity);
    return breadcrumbs;
}

function GlossaryEntitiesPath() {
    const entityRegistry = useEntityRegistry();
    const { entityData } = useGlossaryEntityData();

    const breadcrumbs = getGlossaryBreadcrumbs(entityData?.parentNodes, entityData as GlossaryNode | GlossaryTerm);

    return (
        <PathWrapper>
            <BreadcrumbsWrapper separator=">">
                <BreadcrumbItem>
                    <Link to={PageRoutes.GLOSSARY}>Glossary</Link>
                </BreadcrumbItem>
                {breadcrumbs &&
                    breadcrumbs.map((node) => {
                        return (
                            <BreadcrumbItem key={node.urn}>
                                <Link to={`${entityRegistry.getEntityUrl(node.type, node.urn)}`}>
                                    {entityRegistry.getDisplayName(node.type, node)}
                                </Link>
                            </BreadcrumbItem>
                        );
                    })}
            </BreadcrumbsWrapper>
        </PathWrapper>
    );
}

export default GlossaryEntitiesPath;
