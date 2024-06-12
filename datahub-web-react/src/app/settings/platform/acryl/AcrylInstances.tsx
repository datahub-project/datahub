import React, { useState } from 'react';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import AddOutlinedIcon from '@mui/icons-material/AddOutlined';
import DeleteOutlinedIcon from '@mui/icons-material/DeleteOutlined';
import { Button, Divider, Empty, Skeleton } from 'antd';
import { useGetSearchResultsForMultipleQuery } from '../../../../graphql/search.generated';
import { DataHubConnection, EntityType } from '../../../../types.generated';
import { PLATFORM_FILTER_NAME } from '../../../searchV2/utils/constants';
import { PLATFORM_CONNECTION_URN } from '../../../shared/constants';
import acrylLogo from '../../../../images/acryl-dark-mark.svg';
import NewInstanceForm from './NewInstanceForm';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import { BackButton } from '../../../sharedV2/buttons/BackButton';
import { HorizontalListSkeletons } from '../../../homeV2/content/HorizontalListSkeletons';
import ImageWithColoredBackground from '../../../previewV2/ImageWIthColoredBackground';
import { useDeleteConnectionMutation } from '../../../../graphql/connection.generated';
import { ToastType, showToastMessage } from '../../../sharedV2/toastMessageUtils';
import { ConfirmationModal } from '../../../sharedV2/modals/ConfirmationModal';
import { removeFromInstancesList } from './cacheUtils';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const HeaderContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 85px;
    padding: 20px;
`;

const HeaderTitle = styled.div`
    display: flex;
    font-size: 20px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

const HeaderSubtext = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.SUB_TEXT};
`;

const RightContainer = styled.div`
    display: flex;
    align-items: center;
`;

const LeftContainer = styled.div`
    display: flex;
    align-items: center;
`;

const InstancesList = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    grid-gap: 40px;
    margin: 40px;
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
`;

const Instance = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
    font-size: 20px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
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
        border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};

        ${DeleteIcon} {
            display: block !important;
        }
    }
`;

const PrimaryButton = styled(Button)`
    color: ${REDESIGN_COLORS.WHITE};
    font-size: 12px;
    box-shadow: none;
    border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
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
        border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
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

const StyledEmpty = styled(Empty)`
    color: ${REDESIGN_COLORS.TEXT_GREY};
    padding-top: 80px;
    width: 100%;
`;

const AcrylInstances = () => {
    const history = useHistory();
    const [deleteConnection] = useDeleteConnectionMutation();

    const inputs = {
        types: [EntityType.DatahubConnection],
        query: '*',
        start: 0,
        count: 50,
        orFilters: [{ and: [{ field: PLATFORM_FILTER_NAME, values: [PLATFORM_CONNECTION_URN] }] }],
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

    const hasHistory = (history as any)?.length > 2;

    const goBack = () => {
        (history as any).goBack();
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
                            {hasHistory && <BackButton onGoBack={goBack} />}
                            <Header>
                                <HeaderTitle> Acryl</HeaderTitle>
                                <HeaderSubtext>Integrate with other Acryl instances</HeaderSubtext>
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
                        <InstancesList>
                            {searchAcrossEntities?.searchResults.map((result) => {
                                const entity = result?.entity as DataHubConnection;
                                return (
                                    <Instance onClick={() => openEditInstance(entity)}>
                                        <ImageWithColoredBackground
                                            src={acrylLogo}
                                            imgSize={40}
                                            backgroundSize={72}
                                            borderRadius={16}
                                        />
                                        {entity.details?.name || entity.urn}
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
                            {searchAcrossEntities?.searchResults.length === 0 && <StyledEmpty />}
                        </InstancesList>
                    )}
                </>
            )}
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleDeleteClose}
                handleConfirm={handleDeleteConfirm}
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to delete the connection?"
                isDeleteModal
            />
        </Container>
    );
};

export default AcrylInstances;
