import { Button, Card, Divider, Input, Modal, Pagination } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { ExclamationCircleFilled, LoadingOutlined, PlusOutlined, SearchOutlined } from '@ant-design/icons';
import { useBaseEntity } from '../../../EntityContext';
import './RelationshipsTab.less';
import { EntityType, Join } from '../../../../../../types.generated';
import { useGetSearchResultsQuery } from '../../../../../../graphql/search.generated';
import {
    GetDatasetQuery,
    useGetDatasetLazyQuery,
    useGetDatasetSchemaLazyQuery,
} from '../../../../../../graphql/dataset.generated';
import { useGetEntityWithSchema } from '../Schema/useGetEntitySchema';
import closeIcon from '../../../../../../images/close_dark.svg';
import { CreateJoinModal } from '../../../components/styled/Join/CreateJoinModal';
import { JoinPreview } from '../../../components/styled/Join/JoinPreview';
import { SearchSelectModal } from '../../../components/styled/search/SearchSelectModal';

const StyledPagination = styled(Pagination)`
    margin: 0px;
    padding: 0px;
    padding-left: 400px;
`;
const StyledInput = styled(Input)`
    border-radius: 70px;
    border: 1px solid rgba(0, 0, 0, 0.12);
    max-width: 416px;
    height: 40px !important;
`;
const ThinDivider = styled(Divider)`
    height: 1px;
    width: 520px !important;
    background: #f0f0f0;
    margin-left: -70px;
    margin-bottom: 0px;
`;

export const RelationshipsTab = () => {
    const [pageSize, setPageSize] = useState(10);
    const [currentPage, setCurrentPage] = useState(0);
    const [filterText, setFilterText] = useState('');
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    // Dynamically load the schema + editable schema information.
    const { entityWithSchema } = useGetEntityWithSchema();
    const [modalVisible, setModalVisible] = useState(false);
    const [joinModalVisible, setjoinModalVisible] = useState(false);
    const tabs = [
        {
            key: 'joinsTab',
            tab: 'Joins',
        },
    ];
    const {
        data: joins,
        loading: loadingJoin,
        error: errorJoin,
    } = useGetSearchResultsQuery({
        variables: {
            input: {
                type: EntityType.Join,
                query: `${filterText ? `${filterText}` : ''}`,
                orFilters: [
                    {
                        and: [
                            {
                                field: 'datasetA',
                                values: [baseEntity?.dataset?.urn || ''],
                            },
                        ],
                    },
                    {
                        and: [
                            {
                                field: 'datasetB',
                                values: [baseEntity?.dataset?.urn || ''],
                            },
                        ],
                    },
                ],
                start: currentPage * pageSize,
                count: pageSize, // all matched joins
            },
        },
    });
    const totalResults = joins?.search?.total || 0;
    let joinData: Join[] = [];
    if (loadingJoin) {
        joinData = [{}] as Join[];
    }

    if (!loadingJoin && joins?.search && joins?.search?.searchResults?.length > 0 && !errorJoin) {
        joinData = joins.search.searchResults.map((r) => r.entity as Join);
    }

    const contentListNoTitle: Record<string, React.ReactNode> = {
        joinsTab:
            joinData.length > 0 ? (
                joinData.map((record) => {
                    return (
                        <>
                            <div>
                                <JoinPreview
                                    joinData={record}
                                    baseEntityUrn={baseEntity?.dataset?.urn}
                                    prePageType="Dataset"
                                />
                                <Divider className="thin-divider" />
                            </div>
                        </>
                    );
                })
            ) : (
                <>
                    {!loadingJoin && (
                        <div>
                            <h5>No join available yet</h5>
                        </div>
                    )}
                    {loadingJoin && (
                        <div>
                            Joins <LoadingOutlined />
                        </div>
                    )}
                </>
            ),
    };
    const [activeTabKey, setActiveTabKey] = useState<string>('joinsTab');
    const onTabChange = (key: string) => {
        setActiveTabKey(key);
    };
    const [table2LazySchema, setTable2LazySchema] = useState(undefined as any);
    const [getTable2LazySchema] = useGetDatasetSchemaLazyQuery({
        onCompleted: (data) => {
            setTable2LazySchema(data);
        },
    });
    const [table2LazyDataset, setTable2LazyDataset] = useState(undefined as any);
    const [getTable2LazyDataset] = useGetDatasetLazyQuery({
        onCompleted: (data) => {
            setTable2LazyDataset(data);
        },
    });

    const schemaIssueModal = () => {
        Modal.error({
            title: `Schema error`,
            className: 'schema-modal',
            content: (
                <div>
                    <ThinDivider />
                    <p className="msg-div-inner">
                        A schema was not ingested for the dataset selected. Join cannot be created.
                    </p>
                    <ThinDivider />
                </div>
            ),
            onOk() {},
            okText: 'Ok',
            icon: <ExclamationCircleFilled />,
            closeIcon: <img src={closeIcon} alt="" />,
            maskClosable: true,
            closable: true,
        });
    };
    useEffect(() => {
        if (
            table2LazySchema?.dataset !== undefined &&
            table2LazySchema?.dataset?.schemaMetadata?.fields === undefined
        ) {
            schemaIssueModal();
        }
        if (
            table2LazySchema?.dataset !== undefined &&
            table2LazySchema?.dataset?.schemaMetadata?.fields !== undefined
        ) {
            setjoinModalVisible(false);
            setModalVisible(true);
        }
    }, [table2LazySchema]);
    return (
        <>
            {joinModalVisible && (
                <SearchSelectModal
                    titleText="Select Table 2"
                    continueText="Submit"
                    onContinue={async (selectedDataSet) => {
                        await getTable2LazySchema({
                            variables: {
                                urn: selectedDataSet[0] || '',
                            },
                        });
                        await getTable2LazyDataset({
                            variables: {
                                urn: selectedDataSet[0] || '',
                            },
                        });
                    }}
                    onCancel={() => setjoinModalVisible(false)}
                    fixedEntityTypes={[EntityType.Dataset]}
                    singleSelect
                    hideToolbar
                />
            )}
            {baseEntity !== undefined && (
                <CreateJoinModal
                    table1={baseEntity}
                    table1Schema={entityWithSchema}
                    table2={table2LazyDataset}
                    table2Schema={table2LazySchema}
                    visible={modalVisible}
                    setModalVisible={setModalVisible}
                    onCancel={() => {
                        setModalVisible(false);
                    }}
                />
            )}
            <Card
                headStyle={{ border: '2px', fontSize: '16px' }}
                className="RelationshipsTab"
                tabList={tabs}
                activeTabKey={activeTabKey}
                onTabChange={(key) => {
                    onTabChange(key);
                }}
            >
                <div className="search-header-div">
                    <StyledInput
                        defaultValue={filterText}
                        placeholder="Find join..."
                        onChange={(e) => setFilterText(e.target.value)}
                        allowClear
                        autoFocus
                        prefix={<SearchOutlined />}
                    />
                    <Button
                        type="link"
                        className="add-btn-link"
                        hidden={entityWithSchema?.schemaMetadata?.fields === undefined}
                        onClick={() => {
                            setjoinModalVisible(true);
                        }}
                    >
                        <PlusOutlined /> Add Join
                    </Button>
                </div>{' '}
                <br />
                <br />
                {contentListNoTitle[activeTabKey]}
                {totalResults >= 1 && (
                    <StyledPagination
                        current={currentPage + 1}
                        defaultCurrent={1}
                        pageSize={pageSize}
                        total={totalResults}
                        showLessItems
                        onChange={(page) => setCurrentPage(page - 1)}
                        showSizeChanger={totalResults > 10}
                        onShowSizeChange={(_currNum, newNum) => setPageSize(newNum)}
                        pageSizeOptions={['10', '20', '50', '100']}
                    />
                )}
            </Card>
        </>
    );
};
