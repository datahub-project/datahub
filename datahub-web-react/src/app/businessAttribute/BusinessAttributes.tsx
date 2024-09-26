import React, { useState, useMemo } from 'react';
import styled from 'styled-components';
import { Button, Empty, message, Pagination, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { AlignType } from 'rc-table/lib/interface';
import { Link } from 'react-router-dom';
import { useListBusinessAttributesQuery } from '../../graphql/businessAttribute.generated';
import { Message } from '../shared/Message';
import TabToolbar from '../entity/shared/components/styled/TabToolbar';
import { StyledTable } from '../entity/shared/components/styled/StyledTable';
import CreateBusinessAttributeModal from './CreateBusinessAttributeModal';
import { scrollToTop } from '../shared/searchUtils';
import { useUserContext } from '../context/useUserContext';
import { BusinessAttribute } from '../../types.generated';
import { SearchBar } from '../search/SearchBar';
import { useEntityRegistry } from '../useEntityRegistry';
import useTagsAndTermsRenderer from './utils/useTagsAndTermsRenderer';
import useDescriptionRenderer from './utils/useDescriptionRenderer';
import BusinessAttributeItemMenu from './BusinessAttributeItemMenu';

function BusinessAttributeListMenuColumn(handleDelete: () => void) {
    return (record: BusinessAttribute) => (
        <BusinessAttributeItemMenu title={record.properties?.name} urn={record.urn} onDelete={() => handleDelete()} />
    );
}

const SourceContainer = styled.div`
    width: 100%;
    padding-top: 20px;
    padding-right: 40px;
    padding-left: 40px;
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

const BusinessAttributesContainer = styled.div`
    padding-top: 0px;
`;

const BusinessAttributeHeaderContainer = styled.div`
    && {
        padding-left: 0px;
    }
`;

const BusinessAttributeTitle = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const searchBarStyle = {
    maxWidth: 220,
    padding: 0,
};

const searchBarInputStyle = {
    height: 32,
    fontSize: 12,
};

const DEFAULT_PAGE_SIZE = 10;

export const BusinessAttributes = () => {
    const [isCreatingBusinessAttribute, setIsCreatingBusinessAttribute] = useState(false);
    const entityRegistry = useEntityRegistry();

    // Current User Urn
    const authenticatedUser = useUserContext();

    const canCreateBusinessAttributes = authenticatedUser?.platformPrivileges?.createBusinessAttributes;
    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [tagHoveredUrn, setTagHoveredUrn] = useState<string | undefined>(undefined);

    const {
        loading: businessAttributeLoading,
        error: businessAttributeError,
        data: businessAttributeData,
        refetch: businessAttributeRefetch,
    } = useListBusinessAttributesQuery({
        variables: {
            start,
            count: pageSize,
            query,
        },
    });
    const descriptionRender = useDescriptionRenderer(businessAttributeRefetch);
    const tagRenderer = useTagsAndTermsRenderer(
        tagHoveredUrn,
        setTagHoveredUrn,
        {
            showTags: true,
            showTerms: false,
        },
        query || '',
        businessAttributeRefetch,
    );

    const termRenderer = useTagsAndTermsRenderer(
        tagHoveredUrn,
        setTagHoveredUrn,
        {
            showTags: false,
            showTerms: true,
        },
        query || '',
        businessAttributeRefetch,
    );

    const totalBusinessAttributes = businessAttributeData?.listBusinessAttributes?.total || 0;
    const businessAttributes = useMemo(
        () => (businessAttributeData?.listBusinessAttributes?.businessAttributes || []) as BusinessAttribute[],
        [businessAttributeData],
    );

    const onTagTermCell = (record: BusinessAttribute) => ({
        onMouseEnter: () => {
            setTagHoveredUrn(record.urn);
        },
        onMouseLeave: () => {
            setTagHoveredUrn(undefined);
        },
    });

    const handleDelete = () => {
        setTimeout(() => {
            businessAttributeRefetch?.();
        }, 2000);
    };
    const tableData = businessAttributes || [];
    const tableColumns = [
        {
            width: '20%',
            title: 'Name',
            dataIndex: ['properties', 'name'],
            key: 'name',
            render: (name: string, record: any) => (
                <Link to={`${entityRegistry.getEntityUrl(record.type, record.urn)}`}>{name}</Link>
            ),
        },
        {
            title: 'Description',
            dataIndex: ['properties', 'description'],
            key: 'description',
            width: '20%',
            // render: (description: string) => description || '',
            render: descriptionRender,
        },
        {
            width: '20%',
            title: 'Tags',
            dataIndex: ['properties', 'tags'],
            key: 'tags',
            render: tagRenderer,
            onCell: onTagTermCell,
        },
        {
            width: '20%',
            title: 'Glossary Terms',
            dataIndex: ['properties', 'glossaryTags'],
            key: 'glossaryTags',
            render: termRenderer,
            onCell: onTagTermCell,
        },
        {
            width: '13%',
            title: 'Data Type',
            dataIndex: ['properties', 'businessAttributeDataType'],
            key: 'businessAttributeDataType',
            render: (dataType: string) => dataType || '',
        },
        {
            title: '',
            dataIndex: '',
            width: '5%',
            align: 'right' as AlignType,
            key: 'menu',
            render: BusinessAttributeListMenuColumn(handleDelete),
        },
    ];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    return (
        <SourceContainer>
            {businessAttributeLoading && !businessAttributeData && (
                <Message type="loading" content="Loading businessAttributes..." style={{ marginTop: '10%' }} />
            )}
            {businessAttributeError && message.error('Failed to load businessAttributes :(')}
            <BusinessAttributesContainer>
                <BusinessAttributeHeaderContainer>
                    <BusinessAttributeTitle level={2}>Business Attribute</BusinessAttributeTitle>
                    <Typography.Paragraph type="secondary">View your Business Attributes</Typography.Paragraph>
                </BusinessAttributeHeaderContainer>
            </BusinessAttributesContainer>
            <TabToolbar>
                <Button
                    type="text"
                    onClick={() => setIsCreatingBusinessAttribute(true)}
                    data-testid="add-business-attribute-button"
                    disabled={!canCreateBusinessAttributes}
                >
                    <PlusOutlined /> Create Business Attribute
                </Button>
                <SearchBar
                    initialQuery=""
                    placeholderText="Search Business Attributes..."
                    suggestions={[]}
                    style={searchBarStyle}
                    inputStyle={searchBarInputStyle}
                    onSearch={() => null}
                    onQueryChange={(q) => setQuery(q.length > 0 ? q : undefined)}
                    entityRegistry={entityRegistry}
                />
            </TabToolbar>
            <StyledTable
                columns={tableColumns}
                dataSource={tableData}
                rowKey="urn"
                locale={{
                    emptyText: <Empty description="No Business Attributes!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                pagination={false}
            />
            <PaginationContainer>
                <Pagination
                    style={{ margin: 40 }}
                    current={page}
                    pageSize={pageSize}
                    total={totalBusinessAttributes}
                    showLessItems
                    onChange={onChangePage}
                    showSizeChanger={false}
                />
            </PaginationContainer>
            <CreateBusinessAttributeModal
                open={isCreatingBusinessAttribute}
                onClose={() => setIsCreatingBusinessAttribute(false)}
                onCreateBusinessAttribute={() => {
                    businessAttributeRefetch?.();
                }}
            />
        </SourceContainer>
    );
};
