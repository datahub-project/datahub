import React from 'react';
import { List } from 'antd';
import styled from 'styled-components';
import { RecommendationContent, RecommendationRenderType } from '../../../types.generated';
import { PreviewType } from '../../entity/Entity';
import EntityRegistry from '../../entity/EntityRegistry';
import { RecommendationDisplayType, RecommendationsRenderer } from './RecommendationsRenderer';

const StyledList = styled(List)`
    padding-left: 40px;
    padding-right: 40px;
    margin-top: -1px;
    .ant-list-items > .ant-list-item {
        padding-right: 0px;
        padding-left: 0px;
    }
    > .ant-list-header {
        padding-right: 0px;
        padding-left: 0px;
        font-size: 14px;
        font-weight: 600;
        margin-left: -20px;
        border-bottom: none;
        padding-bottom: 0px;
        padding-top: 15px;
    }
` as typeof List;

const StyledListItem = styled(List.Item)`
    padding-top: 20px;
`;

export class EntityListRenderer implements RecommendationsRenderer {
    entityRegistry;

    constructor(entityRegistry: EntityRegistry) {
        this.entityRegistry = entityRegistry;
    }

    renderRecommendation(
        moduleType: string,
        renderType: RecommendationRenderType,
        content: RecommendationContent[],
        displayType: RecommendationDisplayType,
    ): JSX.Element {
        // todo: track clicks via module type.
        console.log(moduleType);
        console.log(renderType);
        console.log(displayType);
        const entities = content.map((cnt) => cnt.entity);
        return (
            <StyledList
                bordered
                dataSource={entities}
                renderItem={(item) =>
                    item && (
                        <StyledListItem>
                            {this.entityRegistry.renderPreview(item.type, PreviewType.PREVIEW, item)}
                        </StyledListItem>
                    )
                }
            />
        );
    }
}
