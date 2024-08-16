import React from 'react';
import Icon, {
    ApartmentOutlined,
    DatabaseOutlined,
    DeploymentUnitOutlined,
    FilterOutlined,
    LineChartOutlined,
} from '@ant-design/icons';
import ViewComfyOutlinedIcon from '@mui/icons-material/ViewComfyOutlined';
import TableauWorkbookLogo from '../../../../images/tableau-workbook.svg?react';
import TableauEmbeddedDataSourceLogo from '../../../../images/tableau-embedded-data-source.svg?react';
import TableauPublishedDataSourceLogo from '../../../../images/tableau-published-data-source.svg?react';

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
}

export const TYPE_ICON_CLASS_NAME = 'typeIcon';

export function getSubTypeIcon(subType?: string): JSX.Element | undefined {
    if (!subType) return undefined;
    const lowerSubType = subType.toLowerCase();
    if (lowerSubType === SubType.Database.toLowerCase()) {
        return <DatabaseOutlined className={TYPE_ICON_CLASS_NAME} />;
    }
    if (lowerSubType === SubType.Schema.toLowerCase()) {
        return <ApartmentOutlined className={TYPE_ICON_CLASS_NAME} />;
    }
    if (lowerSubType === SubType.Dataset.toLowerCase()) {
        return <ApartmentOutlined className={TYPE_ICON_CLASS_NAME} />;
    }
    if (lowerSubType === SubType.Project.toLowerCase()) {
        return <DeploymentUnitOutlined className={TYPE_ICON_CLASS_NAME} />;
    }
    if (lowerSubType === SubType.Table.toLowerCase()) {
        return <ViewComfyOutlinedIcon fontSize="inherit" className={TYPE_ICON_CLASS_NAME} />;
    }
    if (lowerSubType === SubType.View.toLowerCase()) {
        return <FilterOutlined className={TYPE_ICON_CLASS_NAME} />;
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
        return <LineChartOutlined className={TYPE_ICON_CLASS_NAME} />;
    }
    return undefined;
}
