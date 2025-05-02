import AddOutlinedIcon from '@mui/icons-material/AddOutlined';
import DeleteOutlinedIcon from '@mui/icons-material/DeleteOutlined';
import { Button, Divider, Modal, Skeleton, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entity/shared/constants';
import { Body, Layout, PrimaryHeading } from '@app/govern/Dashboard/components';
import { HorizontalListSkeletons } from '@app/homeV2/content/HorizontalListSkeletons';
import ImageWithColoredBackground from '@app/previewV2/ImageWIthColoredBackground';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { PlatformIntegrationBreadcrumb } from '@app/settings/platform/PlatformIntegrationBreadcrumb';
import NewInstanceForm from '@app/settings/platform/acryl/NewInstanceForm';
import { removeFromInstancesList } from '@app/settings/platform/acryl/cacheUtils';
import { PLATFORM_CONNECTION_URN } from '@app/shared/constants';
import { ToastType, showToastMessage } from '@app/sharedV2/toastMessageUtils';

import { useDeleteConnectionMutation } from '@graphql/connection.generated';
import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { DataHubConnection, EntityType } from '@types';

import acrylLogo from '@images/acryl-dark-mark.svg';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    height: 100%;
`;

const HeaderContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 20px;
`;

const RightContainer = styled.div`
    display: flex;
    align-items: center;
`;

const LeftContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const InstancesList = styled.div<{ isEmptyState?: boolean }>`
    display: grid;
    grid-template-columns: ${(props) => !props.isEmptyState && 'repeat(3, minmax(250px, 1fr))'};
    grid-gap: 40px;
    margin: ${(props) => !props.isEmptyState && '40px'};
    height: ${(props) => props.isEmptyState && '100%'};
    padding-bottom: ${(props) => !props.isEmptyState && '40px'};
`;

const Header = styled.div`
    display: flex;
    flex-direction: column;
`;

const DeleteIcon = styled(DeleteOutlinedIcon)`
    position: absolute;
    height: 18px !important;
    right: 2px;
    top: 12px;
    display: none !important;
    color: ${REDESIGN_COLORS.BLUE};
`;

const Instance = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
    font-size: 20px;
    font-weight: 500;
    border-radius: 18px;
    padding: 12px;
    border: 1px solid transparent;
    position: relative;

    svg {
        height: 45px;
        width: 45px;
    }

    :hover {
        cursor: pointer;
        border: 1px solid ${REDESIGN_COLORS.BLUE};

        ${DeleteIcon} {
            display: block !important;
        }
    }
`;

const InstanceName = styled.div`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const PrimaryButton = styled(Button)`
    color: #fff;
    font-size: 12px;
    box-shadow: none;
    margin-left: 9px;
    padding: 10px 20px;
    display: flex;
    gap: 5px;
    align-items: center;

    svg {
        height: 18px;
        width: 18px;
    }

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
    }
`;

const StyledDivider = styled(Divider)`
    margin: 0;
`;

const SkeletonCard = styled(Skeleton.Button)<{ width: string }>`
    &&& {
        height: 100px;
        width: 260px;
    }
`;

const SkeletonContainer = styled.div`
    display: flex;
    margin: 20px;
`;

const FlexWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex: 1;

    p:not(:first-child) {
        font-size: 16px;
        margin-bottom: 0;
    }
`;

const InstancesListEmptyState = () => (
    <Layout>
        <Body>
            <FlexWrapper>
                <div style={{ textAlign: 'center', fontSize: '18px' }}>
                    <PrimaryHeading>Your DataHub Cloud Instances</PrimaryHeading>
                    <p style={{ marginTop: '1rem' }}>
                        {` Looks like you haven't set up any connections to other DataHub Cloud instances.`}
                    </p>
                    <p>Add a new connection using the button on the top right of this page!</p>
                </div>
            </FlexWrapper>
        </Body>
    </Layout>
);

const AcrylInstances = () => {
    const [deleteConnection] = useDeleteConnectionMutation();

    const inputs = {
        types: [EntityType.DatahubConnection],
        query: '*',
        start: 0,
        count: 50,
        orFilters: [{ and: [{ field: PLATFORM_FILTER_NAME, values: [PLATFORM_CONNECTION_URN] }] }],
        searchFlags: { skipCache: true },
    };

    // Execute search
    const {
        data: searchData,
        loading: isLoading,
        client,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
    });

    const searchAcrossEntities = searchData?.searchAcrossEntities;

    const [openNewInstance, setOpenNewInstance] = useState<boolean>(false);
    const [isEditForm, setIsEditForm] = useState<boolean>(false);
    const [currentInstance, setCurrentInstance] = useState<DataHubConnection | undefined>();
    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);
    const [instanceToDelete, setInstanceToDelete] = useState<string>('');

    const addButtonClick = () => {
        setIsEditForm(false);
        setOpenNewInstance(true);
    };

    const openEditInstance = (entity: DataHubConnection) => {
        setCurrentInstance(entity);
        setOpenNewInstance(true);
        setIsEditForm(true);
    };

    const deleteInstance = async () => {
        deleteConnection({
            variables: {
                input: {
                    urn: instanceToDelete,
                },
            },
        })
            .then(() => {
                showToastMessage(ToastType.SUCCESS, 'Connection deleted successfully!', 3);
                setInstanceToDelete('');
                removeFromInstancesList(client, inputs, instanceToDelete, searchAcrossEntities);
            })
            .catch(() => {
                showToastMessage(ToastType.ERROR, 'Failed to delete the connection', 3);
                setInstanceToDelete('');
            });
    };

    const handleDeleteConfirm = () => {
        deleteInstance();
        setShowConfirmDelete(false);
    };

    const handleDeleteClose = () => {
        setShowConfirmDelete(false);
    };

    return (
        <Container>
            {openNewInstance ? (
                <NewInstanceForm
                    setOpenNewInstance={setOpenNewInstance}
                    isEditForm={isEditForm}
                    selectedInstance={currentInstance}
                    inputs={inputs}
                    searchAcrossEntities={searchAcrossEntities}
                />
            ) : (
                <>
                    <HeaderContainer>
                        <LeftContainer>
                            <PlatformIntegrationBreadcrumb name="DataHub Cloud" />
                            <Header>
                                <Typography.Title level={3}>DataHub Cloud</Typography.Title>
                                <Typography.Text type="secondary">
                                    Integrate with another DataHub Cloud instance to enable data sharing
                                </Typography.Text>
                            </Header>
                        </LeftContainer>
                        <RightContainer>
                            <PrimaryButton type="primary" size="large" onClick={() => addButtonClick()}>
                                <AddOutlinedIcon /> Add a connection
                            </PrimaryButton>
                        </RightContainer>
                    </HeaderContainer>
                    <StyledDivider />
                    {isLoading ? (
                        <SkeletonContainer>
                            <HorizontalListSkeletons Component={SkeletonCard} showHeader={false} />
                        </SkeletonContainer>
                    ) : (
                        <InstancesList isEmptyState={searchAcrossEntities?.searchResults?.length === 0}>
                            {searchAcrossEntities?.searchResults?.map((result) => {
                                const entity = result?.entity as DataHubConnection;
                                return (
                                    <Instance onClick={() => openEditInstance(entity)}>
                                        <ImageWithColoredBackground
                                            src={acrylLogo}
                                            imgSize={30}
                                            backgroundSize={54}
                                            borderRadius={16}
                                        />
                                        <InstanceName>{entity.details?.name || entity.urn}</InstanceName>
                                        <DeleteIcon
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                setInstanceToDelete(entity.urn);
                                                setShowConfirmDelete(true);
                                            }}
                                        />
                                    </Instance>
                                );
                            })}
                            {searchAcrossEntities?.searchResults?.length === 0 && <InstancesListEmptyState />}
                        </InstancesList>
                    )}
                </>
            )}
            <Modal
                open={showConfirmDelete}
                onCancel={handleDeleteConfirm}
                onOk={handleDeleteClose}
                title="Confirm Delete"
                okText="No"
                cancelText="Yes"
            >
                Are you sure you want to delete the connection?
            </Modal>
        </Container>
    );
};

export default AcrylInstances;
