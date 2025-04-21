// import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined';
import { useApolloClient } from '@apollo/client';
import { Divider, Form, Input, message } from 'antd';
import React, { useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { updateInstancesList } from '@app/settingsV2/platform/acryl/cacheUtils';
import {
    HeaderContainer,
    HeaderSubtext,
    HeaderTitle,
    LeftContainer,
} from '@app/settingsV2/platform/acryl/styledComponents';
import {
    ACRYL_PLATFORM_URN,
    getConnectionBlob,
    getTokenFromJson,
    getURLFromJson,
    showToken,
} from '@app/settingsV2/platform/acryl/utils';
import { FormItemTitle, StyledButton, StyledFormItem } from '@app/shared/share/v2/styledComponents';
import { BackButton } from '@app/sharedV2/buttons/BackButton';

import { useUpdateConnectionMutation, useUpsertConnectionMutation } from '@graphql/connection.generated';
import { DataHubConnection, DataHubConnectionDetailsType, SearchAcrossEntitiesInput, SearchResults } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    height: 100%;
`;

const StyledForm = styled(Form)`
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 16px;
`;

const FormItemContainer = styled.div`
    display: flex;
    flex-direction: column;
    font-size: 14px;
    width: 50%;
`;

const FooterContainer = styled.div`
    width: 100%;
    display: flex;
    justify-content: end;
    flex-grow: 1;
    align-items: end;
    flex-direction: column;
`;

const ButtonsContainer = styled.div`
    display: flex;
    padding: 20px;
    gap: 10px;

    .ant-btn {
        font-size: 14px;
    }
    font-size: 14px !important;
`;

const StyledDivider = styled(Divider)`
    margin: 0;
`;

const Header = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    setOpenNewInstance: React.Dispatch<React.SetStateAction<boolean>>;
    isEditForm: boolean;
    selectedInstance?: DataHubConnection;
    inputs: SearchAcrossEntitiesInput;
    searchAcrossEntities?: SearchResults | null;
}

const NewInstanceForm = ({ setOpenNewInstance, isEditForm, selectedInstance, inputs, searchAcrossEntities }: Props) => {
    const { theme } = useCustomTheme();
    const [form] = Form.useForm();
    const history = useHistory();

    const [upsertConnection] = useUpsertConnectionMutation();
    const [updateConnection] = useUpdateConnectionMutation();
    const client = useApolloClient();
    const hasHistory = (history as any)?.length > 2;

    const instanceURL = isEditForm && getURLFromJson(selectedInstance?.details?.json?.blob);
    const initialToken = isEditForm && getTokenFromJson(selectedInstance?.details.json?.blob);

    const [isTokenEdited, setIsTokenEdited] = useState(false);

    const showSuccessMessage = () => {
        message.success({
            content: isEditForm ? 'Successfully updated the Connection.' : 'Successfully added the Connection.',
        });
    };

    const showErrorMessage = () => {
        message.error({
            content: `Failed to ${isEditForm ? 'update' : 'add'} connection. An unexpected error occurred.`,
        });
    };

    const addInstance = async (formData: any) => {
        const newInstance = {
            name: formData.name,
            platformUrn: ACRYL_PLATFORM_URN,
            type: DataHubConnectionDetailsType.Json,
            json: {
                blob: getConnectionBlob(formData.url, formData.token),
            },
        };
        upsertConnection({
            variables: {
                input: newInstance,
            },
        })
            .then((res) => {
                showSuccessMessage();
                setOpenNewInstance(false);
                updateInstancesList(client, inputs, newInstance, res.data?.upsertConnection.urn, searchAcrossEntities);
            })
            .catch(() => {
                showErrorMessage();
            });
    };

    const updateInstance = async (formData: any) => {
        if (selectedInstance?.urn) {
            const updatedInstance = {
                urn: selectedInstance?.urn,
                name: formData.name,
                platformUrn: ACRYL_PLATFORM_URN,
                type: DataHubConnectionDetailsType.Json,
                json: {
                    blob: getConnectionBlob(formData.url, isTokenEdited ? formData.token : initialToken),
                },
            };

            updateConnection({
                variables: {
                    input: updatedInstance,
                },
            })
                .then((res) => {
                    showSuccessMessage();
                    setOpenNewInstance(false);
                    updateInstancesList(
                        client,
                        inputs,
                        updatedInstance,
                        res.data?.updateConnection.urn,
                        searchAcrossEntities,
                    );
                })
                .catch(() => {
                    showErrorMessage();
                });
        }
    };

    const onCancelAdd = () => {
        setOpenNewInstance(false);
    };

    const handleTokenChange = () => {
        setIsTokenEdited(true);
    };

    // Commenting to be used later
    // const testConnection = () => {

    //     console.log('Testing connection');
    // };

    return (
        <Container>
            <HeaderContainer>
                <LeftContainer>
                    {hasHistory && <BackButton onGoBack={() => setOpenNewInstance(false)} />}
                    <Header>
                        <HeaderTitle> {isEditForm ? selectedInstance?.details?.name : 'Add a Connection'}</HeaderTitle>
                        <HeaderSubtext>
                            {isEditForm ? 'Edit connection' : 'Manage Integrations with other Acryl instances'}
                        </HeaderSubtext>
                    </Header>
                </LeftContainer>
            </HeaderContainer>
            <StyledDivider />
            <StyledForm
                form={form}
                name="upsertInstanceForm"
                onFinish={isEditForm ? updateInstance : addInstance}
                initialValues={
                    isEditForm
                        ? { name: selectedInstance?.details?.name, url: instanceURL, token: showToken(initialToken, 5) }
                        : {}
                }
            >
                <FormItemContainer>
                    <FormItemTitle>Name</FormItemTitle>
                    <StyledFormItem
                        name="name"
                        rules={[
                            {
                                required: true,
                                message: 'Please enter the instance name',
                            },
                            { whitespace: true },
                        ]}
                    >
                        <Input placeholder="Instance name" />
                    </StyledFormItem>
                </FormItemContainer>
                <FormItemContainer>
                    <FormItemTitle>URL</FormItemTitle>
                    <StyledFormItem
                        name="url"
                        rules={[
                            {
                                required: true,
                                message: 'Please enter the instance URL',
                            },
                            { whitespace: true },
                        ]}
                    >
                        <Input placeholder="Instance URL" disabled={isEditForm} />
                    </StyledFormItem>
                </FormItemContainer>
                <FormItemContainer>
                    <FormItemTitle>Token</FormItemTitle>
                    <StyledFormItem
                        name="token"
                        rules={[
                            {
                                required: true,
                                message: 'Please enter the token',
                            },
                            { whitespace: true },
                        ]}
                    >
                        <Input placeholder="Instance Token" onChange={handleTokenChange} />
                    </StyledFormItem>
                </FormItemContainer>
                <FormItemContainer>
                    {/* Commenting the Test Connection button for now, will be used later */}
                    {/* <StyledButton
                        $color={REDESIGN_COLORS.TITLE_PURPLE}
                        $hoverColor={REDESIGN_COLORS.HOVER_PURPLE}
                        $type="filled"
                        onClick={testConnection}
                    >
                        <CheckCircleOutlinedIcon />
                        Test Connection
                    </StyledButton> */}
                </FormItemContainer>
            </StyledForm>

            <FooterContainer>
                <StyledDivider />
                <ButtonsContainer>
                    <StyledButton $color={getColor('primary', 500, theme)} onClick={onCancelAdd}>
                        Cancel
                    </StyledButton>
                    <StyledButton
                        $color={getColor('primary', 500, theme)}
                        $hoverColor={REDESIGN_COLORS.HOVER_PURPLE}
                        $type="filled"
                        form="upsertInstanceForm"
                        key="submit"
                        htmlType="submit"
                    >
                        {isEditForm ? 'Update' : 'Add'}
                    </StyledButton>
                </ButtonsContainer>
            </FooterContainer>
        </Container>
    );
};

export default NewInstanceForm;
