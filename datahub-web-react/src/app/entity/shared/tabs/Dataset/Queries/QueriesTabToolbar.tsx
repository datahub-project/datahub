import { Button, Input, Tooltip } from 'antd';
import { PlusOutlined, SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import TabToolbar from '../../../components/styled/TabToolbar';
import { ADD_UNAUTHORIZED_MESSAGE } from './utils/constants';

const StyledInput = styled(Input)`
    border-radius: 70px;
    max-width: 300px;
`;

type Props = {
    addQueryDisabled: boolean;
    onAddQuery: () => void;
    onChangeSearch: (text: any) => void;
};

export default function QueriesTabToolbar({ addQueryDisabled, onAddQuery, onChangeSearch }: Props) {
    const { t } = useTranslation();

    return (
        <TabToolbar>
            <Tooltip
                placement="right"
                title={(addQueryDisabled && ADD_UNAUTHORIZED_MESSAGE) || t('query.addAHighlightedQuery')}
            >
                <Button disabled={addQueryDisabled} type="text" onClick={onAddQuery} data-testid="add-query-button">
                    <PlusOutlined /> {t('query.addQuery')}
                </Button>
            </Tooltip>
            <StyledInput
                placeholder={t('query.searchQuery')}
                onChange={onChangeSearch}
                allowClear
                prefix={<SearchOutlined />}
                data-testid="search-query-input"
            />
        </TabToolbar>
    );
}
