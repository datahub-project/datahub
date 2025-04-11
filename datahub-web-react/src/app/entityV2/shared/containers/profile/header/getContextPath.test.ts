import { GenericEntityProperties } from '@app/entity/shared/types';
import { dataPlatform } from '@src/Mocks';
import { EntityType } from '@types';
import { getContextPath } from './getContextPath';

const PARENT_CONTAINERS: GenericEntityProperties['parentContainers'] = {
    containers: [
        {
            urn: 'urn:li:container:1',
            type: EntityType.Container,
            platform: dataPlatform,
        },
        {
            urn: 'urn:li:container:2',
            type: EntityType.Container,
            platform: dataPlatform,
        },
    ],
    count: 2,
};

const PARENT_DOMAINS: GenericEntityProperties['parentDomains'] = {
    domains: [
        { urn: 'urn:li:domain:1', type: EntityType.Domain },
        { urn: 'urn:li:domain:2', type: EntityType.Domain },
    ],
    count: 2,
};

const PARENT_NODES: GenericEntityProperties['parentNodes'] = {
    nodes: [
        { urn: 'urn:li:glossaryNode:1', type: EntityType.GlossaryNode },
        {
            urn: 'urn:li:glossaryNode:2',
            type: EntityType.GlossaryNode,
        },
    ],
    count: 2,
};

const PARENT: GenericEntityProperties = {
    urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,name,PROD)',
    type: EntityType.Dataset,
    platform: dataPlatform,
};

describe('getContextPath', () => {
    it('returns empty array by default', () => {
        const entityData = {};

        const contextPath = getContextPath(entityData);
        expect(contextPath).toEqual([]);
    });

    it('returns correct context path for entity with parent containers', () => {
        const entityData = {
            parentContainers: PARENT_CONTAINERS,
            parentDomains: PARENT_DOMAINS,
            parentNodes: PARENT_NODES,
            parent: PARENT,
        };

        const contextPath = getContextPath(entityData);
        expect(contextPath).toEqual(PARENT_CONTAINERS.containers);
    });

    it('returns correct context path for entity with parent domains', () => {
        const entityData = {
            parentContainers: null,
            parentDomains: PARENT_DOMAINS,
            parentNodes: PARENT_NODES,
            parent: PARENT,
        };

        const contextPath = getContextPath(entityData);
        expect(contextPath).toEqual(PARENT_DOMAINS.domains);
    });

    it('returns correct context path for entity with parent nodes', () => {
        const entityData = {
            parentContainers: null,
            parentDomains: null,
            parentNodes: PARENT_NODES,
            parent: PARENT,
        };

        const contextPath = getContextPath(entityData);
        expect(contextPath).toEqual(PARENT_NODES.nodes);
    });

    it('returns correct context path for entity with parent', () => {
        const entityData = {
            parentContainers: null,
            parentDomains: null,
            parentNodes: null,
            parent: PARENT,
        };

        const contextPath = getContextPath(entityData);
        expect(contextPath).toEqual([PARENT]);
    });
});
