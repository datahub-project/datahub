import React from 'react';
import styled from 'styled-components';
// import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined';
import { Divider, Form, Input, Typography, message } from 'antd';
import { useHistory } from 'react-router';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import { HeaderContainer, HeaderSubtext, HeaderTitle, LeftContainer } from './styledComponents';
import { DataHubConnection, DataHubConnectionDetailsType } from '../../../../types.generated';
import { BackButton } from '../../../sharedV2/buttons/BackButton';
import { StyledButton } from '../../../shared/share/v2/styledComponents';
import { useUpdateConnectionMutation, useUpsertConnectionMutation } from '../../../../graphql/connection.generated';
import { ACRYL_PLATFORM_URN, getConnectionBlob, getURLfromJson } from './utils';

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

const FormItemTitle = styled(Typography.Text)`
    margin-bottom: 8px;
    font-size: 14px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

const StyledFormItem = styled(Form.Item)`
    margin-bottom: 8px;

    .ant-input {
        font-size: 14px;
        font-weight: 500;
        border-radius: 8px;
        color: ${REDESIGN_COLORS.FOUNDATION_BLUE_5};

        &:hover,
        &:focus,
        &:active {
            border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        }

        &:focus,
        &:active {
            color: ${REDESIGN_COLORS.TITLE_PURPLE};
            box-shadow: 0px 0px 4px 0px rgba(83, 63, 209, 0.5);
        }
    }
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
    refetch?: () => void;
}

const NewInstanceForm = ({ setOpenNewInstance, isEditForm, selectedInstance, refetch }: Props) => {
    const [form] = Form.useForm();
    const history = useHistory();

    const [upsertConnection] = useUpsertConnectionMutation();
    const [updateConnection] = useUpdateConnectionMutation();

    const hasHistory = (history as any)?.length > 2;

    const instanceURL = isEditForm ? getURLfromJson(selectedInstance?.details?.json?.blob) : '';

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
        upsertConnection({
            variables: {
                input: {
                    name: formData.name,
                    platformUrn: ACRYL_PLATFORM_URN,
                    type: DataHubConnectionDetailsType.Json,
                    json: {
                        blob: getConnectionBlob(formData.url, formData.token),
                    },
                },
            },
        })
            .then(() => {
                showSuccessMessage();
                setOpenNewInstance(false);
                setTimeout(() => {
                    refetch?.();
                }, 3000);
            })
            .catch(() => {
                showErrorMessage();
            });
    };

    const updateInstance = async (formData: any) => {
        if (selectedInstance?.urn) {
            updateConnection({
                variables: {
                    input: {
                        urn: selectedInstance?.urn,
                        name: formData.name,
                        platformUrn: ACRYL_PLATFORM_URN,
                        type: DataHubConnectionDetailsType.Json,
                        json: {
                            blob: selectedInstance?.details?.json?.blob || '',
                        },
                    },
                },
            })
                .then(() => {
                    showSuccessMessage();
                    setOpenNewInstance(false);
                    refetch?.();
                })
                .catch(() => {
                    showErrorMessage();
                });
        }
    };

    const onCancelAdd = () => {
        setOpenNewInstance(false);
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
                        <HeaderTitle> {isEditForm ? selectedInstance?.details.name : 'Add a Connection'}</HeaderTitle>
                        <HeaderSubtext>
                            {isEditForm ? 'Edit form' : 'Manage Integrations with other Acryl instances'}
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
                    isEditForm ? { name: selectedInstance?.details.name, url: instanceURL, token: '***********' } : {}
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
                            { min: 3, max: 50 },
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
                        <Input placeholder="Instance Token" disabled={isEditForm} />
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
                    <StyledButton $color={REDESIGN_COLORS.TITLE_PURPLE} onClick={onCancelAdd}>
                        Cancel
                    </StyledButton>
                    <StyledButton
                        $color={REDESIGN_COLORS.TITLE_PURPLE}
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
