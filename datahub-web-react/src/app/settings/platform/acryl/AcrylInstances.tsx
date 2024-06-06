import React, { useState } from 'react';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import AddOutlinedIcon from '@mui/icons-material/AddOutlined';
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

const Instance = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
    font-size: 20px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TITLE_PURPLE};

    svg {
        height: 45px;
        width: 45px;
    }

    :hover {
        cursor: pointer;
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

    // Execute search
    const {
        data: searchData,
        loading: isLoading,
        refetch,
    } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.DatahubConnection],
                query: '*',
                start: 0,
                count: 50,
                orFilters: [{ and: [{ field: PLATFORM_FILTER_NAME, values: [PLATFORM_CONNECTION_URN] }] }],
                searchFlags: {
                    skipCache: true,
                },
            },
        },
    });

    const searchAcrossEntities = searchData?.searchAcrossEntities;

    const [openNewInstance, setOpenNewInstance] = useState<boolean>(false);
    const [isEditForm, setIsEditForm] = useState<boolean>(false);
    const [currentInstance, setCurrentInstance] = useState<DataHubConnection | undefined>();

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

    return (
        <Container>
            {openNewInstance ? (
                <NewInstanceForm
                    setOpenNewInstance={setOpenNewInstance}
                    isEditForm={isEditForm}
                    selectedInstance={currentInstance}
                    refetch={refetch}
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
                                    </Instance>
                                );
                            })}
                            {searchAcrossEntities?.searchResults.length === 0 && <StyledEmpty />}
                        </InstancesList>
                    )}
                </>
            )}
        </Container>
    );
};

export default AcrylInstances;
