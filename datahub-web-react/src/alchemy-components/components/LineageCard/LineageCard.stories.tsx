import { MockedProvider } from '@apollo/client/testing';
import { Icon, Pill, colors } from '@components';
import { BADGE } from '@geometricpanda/storybook-addon-badges';
import { Meta, StoryObj } from '@storybook/react';
import React, { useState } from 'react';
import { BrowserRouter as Router } from 'react-router-dom';
import styled from 'styled-components';

import { Entity as EntityV2 } from '@app/entityV2/Entity';
import EntityRegistry from '@app/entityV2/EntityRegistry';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import NodeWrapper from '@app/lineageV3/NodeWrapper';
import LineageCard from '@app/lineageV3/components/LineageCard';
import HealthIcon from '@app/previewV2/HealthIcon';
import { EntityRegistryContext } from '@src/entityRegistryContext';

import { Dataset, Entity, EntityType, FeatureFlagsConfig, HealthStatus, HealthStatusType } from '@types';

const StyledMenuIcon = styled(Icon)`
    margin: 0 -8px;
`;

const ContentsWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    line-height: 1.5;
    padding: 8px;
`;

const ChildrenTextWrapper = styled.span`
    display: flex;
    gap: 8px;
`;

const meta = {
    title: 'Lineage / LineageCard',
    component: LineageCard,

    // Display Properties
    parameters: {
        layout: 'centered',
        badges: [BADGE.BETA],
        docs: {
            subtitle: 'Cards to display entities on the lineage visualization',
        },
    },

    // Component-level argTypes
    argTypes: {
        name: {
            description: 'Entity name',
        },
    },

    // Define defaults
    args: {
        urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,entity-name,PROD)',
        loading: false,
        type: EntityType.Dataset,
        name: 'Entity name',
        nameHighlight: { text: 'name', color: colors.yellow[200] },
        platformIcons: ['./assets/platforms/snowflakelogo.png', './assets/platforms/dbtlogo.png'],
        properties: {
            type: EntityType.Dataset,
            subTypes: { typeNames: ['View'] },
            browsePathV2: {
                path: [
                    { name: 'First entry hidden' },
                    { name: 'Database' },
                    {
                        entity: {
                            urn: '',
                            type: EntityType.Dataset,
                            properties: { name: 'Schema' },
                        } as Entity,
                        name: 'Schema',
                    },
                ],
            },
        },
        menuActions: [
            <DeprecationIcon
                urn=""
                deprecation={{ deprecated: true, note: 'description' }}
                showUndeprecate={false}
                showText={false}
            />,
            <HealthIcon
                urn=""
                health={[
                    {
                        status: HealthStatus.Pass,
                        type: HealthStatusType.Assertions,
                        message: 'All assertions passing',
                    },
                ]}
                baseUrl=""
            />,
            <StyledMenuIcon icon="DotsThreeVertical" source="phosphor" />,
        ],
        childrenText: (
            <ChildrenTextWrapper>
                Columns
                <Pill label="10" size="xs" />
            </ChildrenTextWrapper>
        ),
        childrenOpen: false,
    },
} satisfies Meta<typeof LineageCard>;

export default meta;

type Story = StoryObj<typeof meta>;

/* eslint-disable @typescript-eslint/lines-between-class-members */
class MockDatasetEntity implements EntityV2<Dataset> {
    type = EntityType.Dataset;
    icon = () => <></>;
    isSearchEnabled = () => true;
    isBrowseEnabled = () => true;
    isLineageEnabled = () => true;
    getPathName = () => '';
    getCollectionName = () => '';
    renderProfile = (_: string) => <></>;
    renderPreview = () => <></>;
    renderSearch = () => <></>;
    displayName = (data: Dataset) => data.properties?.name || data.urn;
    getGenericEntityProperties = (_data: Dataset, _flags?: FeatureFlagsConfig | undefined) => null;
    supportedCapabilities = () => new Set<any>();
    getGraphName = () => '';
}

/* eslint-enable @typescript-eslint/lines-between-class-members */

export const sandbox: Story = {
    tags: ['dev'],
    render: (props) => {
        // eslint-disable-next-line react-hooks/rules-of-hooks
        const [childrenOpen, setChildrenOpen] = useState(false);
        const entityRegistry = new EntityRegistry();
        entityRegistry.register(new MockDatasetEntity());
        return (
            <MockedProvider>
                <Router>
                    <EntityRegistryContext.Provider value={entityRegistry}>
                        <NodeWrapper urn="" selected={false} dragging={false} isGhost={false} isSearchedEntity={false}>
                            <LineageCard
                                {...props}
                                childrenOpen={childrenOpen}
                                toggleChildren={() => setChildrenOpen(!childrenOpen)}
                            >
                                <ContentsWrapper>Contents!</ContentsWrapper>
                            </LineageCard>
                        </NodeWrapper>
                    </EntityRegistryContext.Provider>
                </Router>
            </MockedProvider>
        );
    },
};
