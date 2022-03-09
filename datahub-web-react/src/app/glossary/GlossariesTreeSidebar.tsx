import React, { useEffect, useState } from 'react';
import { Tree } from 'antd';
import { useApolloClient } from '@apollo/client';
import { DownOutlined, FolderOutlined } from '@ant-design/icons';
import { GetBrowseResultsDocument } from '../../graphql/browse.generated';
import { EntityType } from '../../types.generated';

interface DataNode {
    title: string;
    key: string;
    isLeaf?: boolean;
    children?: DataNode[];
}

function updateTreeData(list: DataNode[], key: React.Key, children: DataNode[]): DataNode[] {
    return list.map((node) => {
        if (node.key === key) {
            return {
                ...node,
                children,
            };
        }
        if (node.children) {
            return {
                ...node,
                children: updateTreeData(node.children, key, children),
            };
        }
        return node;
    });
}

export const GlossariesTreeSidebar = () => {
    const client = useApolloClient();
    const [treeData, setTreeData] = useState<DataNode[]>([]);
    const entityType = EntityType.GlossaryTerm;

    const getTerms = (path: string[] = []) => {
        return client.query({
            query: GetBrowseResultsDocument,
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
    };

    useEffect(() => {
        const initiateTreeData = async () => {
            let tData: any = [];

            // query initial nodes
            const { data } = await getTerms();
            if (data && data.browse) {
                tData = data.browse.groups.map(({ name }) => ({
                    title: name,
                    icon: <FolderOutlined />,
                    key: `${name}`,
                    children: [],
                }));
            }

            if (!tData.length) {
                return;
            }

            const children: any = await Promise.all(tData.map((node) => getTerms([node.key])));

            // fetch child terms
            // eslint-disable-next-line no-restricted-syntax
            for (let i = 0; i < children.length; i++) {
                // eslint-disable-next-line @typescript-eslint/no-shadow
                const { data } = children[i];
                if (data && data.browse && data.browse.entities && data.browse.entities.length > 0) {
                    const terms = data.browse.entities.map(({ properties }: any) => ({
                        title: properties.name,
                        icon: <FolderOutlined />,
                        key: `${properties.name}`,
                        isLeaf: true,
                    }));
                    tData = updateTreeData(tData, tData[i].key, terms);
                }
            }
            setTreeData(tData);
        };
        initiateTreeData();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return <Tree showIcon switcherIcon={<DownOutlined />} treeData={treeData} />;
};
