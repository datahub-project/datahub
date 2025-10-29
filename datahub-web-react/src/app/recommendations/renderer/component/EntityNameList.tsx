import { Divider, List } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { getPlatformName } from '@app/entity/shared/utils';
import DefaultPreviewCard from '@app/preview/DefaultPreviewCard';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

export const StyledList = styled(List)`
    overflow-y: auto;
    height: 100%;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    flex: 1;
    .ant-list-items > .ant-list-item {
        padding-right: 0px;
        padding-left: 0px;
    }
    > .ant-list-header {
        padding-right: 0px;
        padding-left: 0px;
        font-size: 14px;
        font-weight: 600;
        margin-left: -20px;
        border-bottom: none;
        padding-bottom: 0px;
        padding-top: 15px;
    }
    &::-webkit-scrollbar {
        height: 12px;
        width: 5px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
` as typeof List;

export const ListItem = styled.div<{ isSelectMode: boolean }>`
    padding-right: 40px;
    padding-left: ${(props) => (props.isSelectMode ? '20px' : '40px')};
    padding-top: 16px;
    padding-bottom: 8px;
    display: flex;
    align-items: center;
`;

export const ThinDivider = styled(Divider)`
    padding: 0px;
    margin: 0px;
`;

type Props = {
    entities: Array<Entity>;
    onClick?: (index: number) => void;
};
