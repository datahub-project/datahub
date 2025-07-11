import { SearchBar } from '@components';
import { Tree } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import ItemsSelector, { TreeNode } from './ItemsSelector';
import useGetDomainOptions from './useDomains';
// import useDomains from './useDomains';

const Wrapper = styled.div``;

export default function AssetsSection() {
    // const { domains } = useDomains();
    const [isInitialized, setIsInitialized] = useState<boolean>(false);
    const [options, setOptions] = useState<TreeNode[]>([])


    const onDomainOptionsFetched = useCallback((newOptions: TreeNode[], parentDomainUrn?: string) => {
        console.log('>>>newOptions', newOptions);
        setOptions(newOptions);
    }, [])

    const { getDomainOptions } = useGetDomainOptions({onCompleted: onDomainOptionsFetched});

    useEffect(() => {
        if (!isInitialized) {
            getDomainOptions();
            setIsInitialized(true);
        }
    }, [isInitialized, getDomainOptions]);

    return (
        <Wrapper>
            Glossary
            <SearchBar />
            <Tree
                checkable
                titleRender={(node) => <>{node.title}111</>}
                // showLine
                treeData={[
                    {
                        title: '0-0',
                        key: '0-0',
                        children: [
                            {
                                title: '0-0-0',
                                key: '0-0-0',
                                children: [
                                    { title: '0-0-0-0', key: '0-0-0-0' },
                                    { title: '0-0-0-1', key: '0-0-0-1' },
                                    { title: '0-0-0-2', key: '0-0-0-2' },
                                ],
                            },
                            {
                                title: '0-0-1',
                                key: '0-0-1',
                                children: [
                                    { title: '0-0-1-0', key: '0-0-1-0' },
                                    { title: '0-0-1-1', key: '0-0-1-1' },
                                    { title: '0-0-1-2', key: '0-0-1-2' },
                                ],
                            },
                            {
                                title: '0-0-2',
                                key: '0-0-2',
                            },
                        ],
                    },
                    {
                        title: '0-1',
                        key: '0-1',
                        children: [
                            { title: '0-1-0-0', key: '0-1-0-0' },
                            { title: '0-1-0-1', key: '0-1-0-1' },
                            { title: '0-1-0-2', key: '0-1-0-2' },
                        ],
                    },
                    {
                        title: '0-2',
                        key: '0-2',
                    },
                ]}
            />
            <ItemsSelector
                options={[
                    {
                        value: 'test1',
                        label: 'test1',
                        children: [
                            {
                                value: 'test1-child1',
                                label: 'test1-child1',
                            },
                            {
                                value: 'test1-child2',
                                label: 'test1-child2',
                                children: [
                                    {
                                        value: 'test1-child2-child1',
                                        label: 'test1-child2-child1',
                                    },
                                    {
                                        value: 'test1-child2-child2',
                                        label: 'test1-child2-child2',
                                    },
                                ],
                            },
                        ],
                    },
                ]}
            />

            <ItemsSelector options={options} loadAsyncChildren={(option => getDomainOptions(option.value))}/>
        </Wrapper>
    );
}
