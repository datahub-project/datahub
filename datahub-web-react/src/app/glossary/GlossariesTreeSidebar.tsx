import React from 'react';
import { Tree } from 'antd';
import { DownOutlined, FolderOutlined } from '@ant-design/icons';
import { useGetBrowseResultsQuery } from '../../graphql/browse.generated';
import { EntityType } from '../../types.generated';

interface DataNode {
    title: string;
    key: string;
    isLeaf?: boolean;
    children?: DataNode[];
}

export const GlossariesTreeSidebar = () => {
    const entityType = EntityType.GlossaryTerm;

    const path = [];

    const { data, loading, error } = useGetBrowseResultsQuery({
        variables: {
            input: {
                type: entityType,
                path,
                start: 0,
                count: 1000,
                filters: null,
            },
        },
    });

    let treeData: DataNode[] = [];

    if (data && data.browse) {
        treeData = data.browse.groups.map(({ name }, index) => ({
            title: name,
            icon: <FolderOutlined />,
            key: `parent-${name}-${index}`,
            children: [],
        }));
    }

    console.log(data, loading, error);

    return <Tree showIcon switcherIcon={<DownOutlined />} treeData={treeData} />;
};
