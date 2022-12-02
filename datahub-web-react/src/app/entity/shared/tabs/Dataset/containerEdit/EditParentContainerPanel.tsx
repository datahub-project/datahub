import { Button, Col, Form, Row } from 'antd';
import Select from 'antd/lib/select';
import axios from 'axios';
import React, { useEffect, useMemo, useState } from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { WhereAmI } from '../../../../../home/whereAmI';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
import { useBaseEntity } from '../../../EntityContext';
import { printErrorMsg, printSuccessMsg } from '../ApiCallUtils';
import { useGetSearchResultsLazyQuery } from '../../../../../../graphql/search.generated';
import { EntityType, SearchResult } from '../../../../../../types.generated';
import { useGetContainerLazyQuery } from '../../../../../../graphql/container.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';

export const EditParentContainerPanel = () => {
    const entityRegistry = useEntityRegistry();
    const urlBase = WhereAmI();
    const updateUrl = `${urlBase}custom/update_containers`;
    const userUrn = FindMyUrn();
    const currUser = FindWhoAmI();
    const userToken = GetMyToken(userUrn);
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const containerValue = baseEntity?.dataset?.container?.properties?.name || '';
    const currUrn = baseEntity && baseEntity.dataset && baseEntity.dataset?.urn;
    const [platform] = useState(baseEntity?.dataset?.platform?.urn || '');
    const [containerPath, setContainerPath] = useState(<div />);
    const [runContainerQuery, { data: containerData }] = useGetContainerLazyQuery();
    const [runQuery, { data: containerCandidates }] = useGetSearchResultsLazyQuery({
        variables: {
            input: {
                type: EntityType.Container,
                query: '*',
                filters: [
                    {
                        field: 'platform',
                        value: platform,
                    },
                ],
                count: 1000,
            },
        },
    });

    useEffect(() => {
        runQuery();
    }, [runQuery]);

    useMemo(() => {
        const selectedName = containerData?.container?.properties?.name || '';
        const containersArray = containerData?.container?.parentContainers?.containers || [];
        const parentArray = containersArray?.map((item) => item?.properties?.name) || [];
        const pathString = parentArray.reverse().join(' => ') || '';
        const outputString1 =
            pathString === '' ? `No parent container to this container ` : `Parent containers to this container: `;
        const outputString2a = pathString === '' ? '' : `${pathString} => `;
        const outputString2b = selectedName === '' ? `` : `${selectedName} (selected)`;
        const finalString2 = (
            <div className="displayContainerPath">
                {outputString1}
                <span style={{ color: 'blue' }}>{outputString2a}</span>
                <span style={{ color: 'red' }}>{outputString2b}</span>
            </div>
        );
        setContainerPath(finalString2);
    }, [containerData?.container?.parentContainers?.containers, containerData?.container?.properties?.name]);

    const containerPool = containerCandidates?.search?.searchResults || [];

    const layout = {
        labelCol: {
            span: 6,
        },
        wrapperCol: {
            span: 14,
        },
    };
    const [formState] = Form.useForm();

    const renderSearchResult = (result: SearchResult) => {
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return displayName;
    };

    const handleChange = (value) => {
        runContainerQuery({
            variables: {
                urn: value,
            },
        });
        formState.setFieldsValue({
            parentContainer: value,
        });
    };

    const resetForm = () => {
        formState.resetFields();
        setContainerPath(<div />);
    };

    const onFinish = async (values) => {
        console.log(values);
        const proposedContainer = values.parentContainer;
        // container is always 1 only, hence list to singular value
        const submission = {
            dataset_name: currUrn,
            requestor: currUser,
            container: proposedContainer,
            user_token: userToken,
        };
        axios
            .post(updateUrl, submission)
            .then((response) => {
                printSuccessMsg(response.status);
                window.location.reload();
            })
            .catch((error) => {
                printErrorMsg(error.toString());
            });
    };

    return (
        <>
            <Form
                name="dynamic_item"
                {...layout}
                form={formState}
                onFinish={onFinish}
                initialValues={{
                    parentContainer: containerValue,
                }}
            >
                <Button type="primary" htmlType="submit">
                    Submit
                </Button>
                &nbsp;
                <Button htmlType="button" onClick={resetForm}>
                    Reset
                </Button>
                <Form.Item
                    name="parentContainer"
                    label="Specify a Container(Optional)"
                    rules={[
                        {
                            required: true,
                            message: 'A container must be specified.',
                        },
                    ]}
                    shouldUpdate={(prevValues, curValues) => prevValues.props !== curValues.props}
                    style={{ marginBottom: '0px' }}
                >
                    <Row>
                        <Col span={6}>
                            <Form.Item>
                                <Select
                                    filterOption
                                    showArrow
                                    placeholder="Search for a parent container.."
                                    onChange={handleChange}
                                >
                                    {containerPool.map((result) => (
                                        <Select.Option key={result?.entity?.urn} value={result?.entity?.urn}>
                                            {renderSearchResult(result)}
                                        </Select.Option>
                                    ))}
                                </Select>
                            </Form.Item>
                        </Col>
                        <Form.Item>
                            <Col>{containerPath}</Col>
                        </Form.Item>
                    </Row>
                </Form.Item>
            </Form>
        </>
    );
};
