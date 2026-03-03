import Icon from '@ant-design/icons';
import { ChartLine, Database as DatabaseIcon, Funnel, Graph, Stack, Table as TableIcon } from '@phosphor-icons/react';
import React from 'react';

import TableauEmbeddedDataSourceLogo from '@images/tableau-embedded-data-source.svg?react';
import TableauPublishedDataSourceLogo from '@images/tableau-published-data-source.svg?react';
import TableauWorkbookLogo from '@images/tableau-workbook.svg?react';
import VertexAIPipelineTaskLogo from '@images/vertexai-pipeline-task.svg?react';

export enum SubType {
    Database = 'Database',
    Schema = 'Schema',
    Dataset = 'Dataset',
    Project = 'Project',
    View = 'View',
    Table = 'Table',
    TableauWorksheet = 'Worksheet',
    TableauWorkbook = 'Workbook',
    TableauPublishedDataSource = 'Published Data Source',
    TableauEmbeddedDataSource = 'Embedded Data Source',
    LookerExplore = 'Explore',
    Looker = 'Look',
    DbtSource = 'Source',
    VertexAIPipelineTask = 'Pipeline Task',
}

export const TYPE_ICON_CLASS_NAME = 'typeIcon';

export function getSubTypeIcon(subType?: string, size?: number | string): JSX.Element | undefined {
    if (!subType) return undefined;
    const iconSize = size || '1em';
    const lowerSubType = subType.toLowerCase();
    if (lowerSubType === SubType.Database.toLowerCase()) {
        return <DatabaseIcon className={TYPE_ICON_CLASS_NAME} size={iconSize} color="currentColor" />;
    }
    if (lowerSubType === SubType.Schema.toLowerCase()) {
        return <Stack className={TYPE_ICON_CLASS_NAME} size={iconSize} color="currentColor" />;
    }
    if (lowerSubType === SubType.Dataset.toLowerCase()) {
        return <TableIcon className={TYPE_ICON_CLASS_NAME} size={iconSize} color="currentColor" />;
    }
    if (lowerSubType === SubType.Project.toLowerCase()) {
        return <Graph className={TYPE_ICON_CLASS_NAME} size={iconSize} color="currentColor" />;
    }
    if (lowerSubType === SubType.View.toLowerCase()) {
        return <Funnel className={TYPE_ICON_CLASS_NAME} size={iconSize} color="currentColor" />;
    }
    if (lowerSubType === SubType.TableauWorkbook.toLowerCase()) {
        return <Icon component={TableauWorkbookLogo} className={TYPE_ICON_CLASS_NAME} />;
    }
    if (lowerSubType === SubType.TableauPublishedDataSource.toLowerCase()) {
        return <Icon component={TableauPublishedDataSourceLogo} className={TYPE_ICON_CLASS_NAME} />;
    }
    if (lowerSubType === SubType.TableauEmbeddedDataSource.toLowerCase()) {
        return <Icon component={TableauEmbeddedDataSourceLogo} className={TYPE_ICON_CLASS_NAME} />;
    }
    if (lowerSubType === SubType.TableauWorksheet.toLowerCase()) {
        return <ChartLine className={TYPE_ICON_CLASS_NAME} size={iconSize} color="currentColor" />;
    }
    if (lowerSubType === SubType.VertexAIPipelineTask.toLowerCase()) {
        return <Icon component={VertexAIPipelineTaskLogo} className={TYPE_ICON_CLASS_NAME} />;
    }
    return undefined;
}
