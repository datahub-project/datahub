import { Button, Card, Divider, Input, Modal, Pagination } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { ExclamationCircleFilled, LoadingOutlined, PlusOutlined, SearchOutlined } from '@ant-design/icons';
import { useBaseEntity } from '../../../EntityContext';
import './RelationTab.less';
import { EntityType, Join } from '../../../../../../types.generated';
import { useGetSearchResultsQuery } from '../../../../../../graphql/search.generated';
import {
    GetDatasetQuery,
    useGetDatasetQuery,
    useGetDatasetSchemaLazyQuery,
    useGetDatasetSchemaQuery,
} from '../../../../../../graphql/dataset.generated';
import { useGetEntityWithSchema } from '../Schema/useGetEntitySchema';
import closeIcon from '../../../../../../images/close_dark.svg';
import { CreateJoinModal } from '../../../components/styled/Join/CreateJoinModal';
import { JoinPreview } from '../../../components/styled/Join/JoinPreview';
import { SearchSelectModal } from '../../../components/styled/search/SearchSelectModal';

type JoinRecord = {
    afield: string;
    bfield: string;
};
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

export const RelationTab = () => {
    const [pageSize, setPageSize] = useState(10);
    const [currentPage, setCurrentPage] = useState(0);
    const [filterText, setFilterText] = useState('');
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    // Dynamically load the schema + editable schema information.
    const { entityWithSchema } = useGetEntityWithSchema();
    const [modalVisible, setModalVisible] = useState(false);
    const [joinModalVisible, setjoinModalVisible] = useState(false);
    // const fieldSortInput: FieldSortInput = {
    //     field: 'lastModifiedAt',
    //     sortOrder: Sort.Desc,
    // };
    const tabs = [
        {
            key: 'joinsTab',
            tab: 'Joins',
        },
        // {
        //     key: 'pkFkTab',
        //     tab: 'PK/FK',
        // },
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

    const joinPreview = (record: Join): JSX.Element => {
        let table1Name;
        let table2Name;
        let table1Urn;
        let table2Urn;
        let shuffleFlag = false;
        const newData = [] as JoinRecord[];
        if (baseEntity?.dataset?.urn === record?.properties?.datasetA?.urn) {
            table1Name = record?.properties?.datasetA?.name;
            table2Name = record?.properties?.datasetB?.name;
            table1Urn = record?.properties?.datasetA?.urn;
            table2Urn = record?.properties?.datasetB?.urn;
            record?.properties?.joinFieldMappings?.fieldMapping?.map((item) => {
                return newData.push({
                    afield: item.afield,
                    bfield: item.bfield,
                });
            });
            shuffleFlag = false;
        } else {
            table1Name = record?.properties?.datasetB?.name;
            table2Name = record?.properties?.datasetA?.name;
            table1Urn = record?.properties?.datasetB?.urn;
            table2Urn = record?.properties?.datasetA?.urn;
            record?.properties?.joinFieldMappings?.fieldMapping?.map((item) => {
                return newData.push({
                    afield: item.bfield,
                    bfield: item.afield,
                });
            });
            shuffleFlag = true;
        }
        const joinHeader = record?.editableProperties?.name || record?.properties?.name || '';
        return (
            <JoinPreview
                joinData={record}
                table1Name={table1Name}
                table2Name={table2Name}
                table1Urn={table1Urn}
                table2Urn={table2Urn}
                joinHeader={joinHeader}
                fieldMap={newData}
                joinDetails={record?.properties?.joinFieldMappings?.details || ''}
                prePageType="Dataset"
                shuffleFlag={shuffleFlag}
            />
        );
    };

    const contentListNoTitle: Record<string, React.ReactNode> = {
        joinsTab:
            joinData.length > 0 ? (
                joinData.map((record) => {
                    return (
                        <>
                            <div>
                                {joinPreview(record)}
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
                            Joins <LoadingOutlined />{' '}
                        </div>
                    )}
                </>
            ),
    };
    const [activeTabKey, setActiveTabKey] = useState<string>('joinsTab');
    const onTabChange = (key: string) => {
        setActiveTabKey(key);
    };
    const [selectDataset, setSelectDataset] = useState<string>('');
    const { data: table2Dataset } = useGetDatasetQuery({
        variables: {
            urn: selectDataset || 'urn:li:dataset:(urn:li:dataPlatform:snowflake,testData,DEV)',
        },
    });
    const { data: table2Schema } = useGetDatasetSchemaQuery({
        variables: {
            urn: selectDataset || 'urn:li:dataset:(urn:li:dataPlatform:snowflake,testData,DEV)',
        },
    });
    const [table2LazySchema, setTable2LazySchema] = useState(undefined as any);
    const [getTable2LazySchema] = useGetDatasetSchemaLazyQuery({
        onCompleted: (data) => {
            setTable2LazySchema(data);
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
                        setSelectDataset(selectedDataSet[0]);
                    }}
                    onCancel={() => setjoinModalVisible(false)}
                    fixedEntityTypes={[EntityType.Dataset]}
                    singleSelect
                />
            )}
            {baseEntity !== undefined && (
                <CreateJoinModal
                    table1={baseEntity}
                    table1Schema={entityWithSchema}
                    table2={table2Dataset}
                    table2Schema={table2Schema}
                    visible={modalVisible}
                    setModalVisible={setModalVisible}
                    onCancel={() => {
                        setModalVisible(false);
                    }}
                />
            )}
            <Card
                // bordered={false}
                headStyle={{ border: '2px', fontSize: '16px' }}
                className="RelationTab"
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
