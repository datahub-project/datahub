import React, { useState } from 'react';
import { Button, Dropdown, Typography } from 'antd';
import { LeftOutlined, RightOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { MenuItemStyle } from '../../../../view/menu/item/styledComponent';

const CustomPaginationContainer = styled.div`
    display: flex;
    flex-direction: row;
    height: 32px;
`;
const NavButton = styled(Button)`
    margin: 4px 6px;
    cursor: pointer;
`;
const DescriptionText = styled(Typography.Text)`
    line-height: 32px;
`;
const VersionText = styled(Typography.Text)`
    padding: 0 4px;
    line-height: 32px;
    cursor: pointer;
`;
const VersionRightText = styled(Typography.Text)`
    padding-left: 4px;
    line-height: 32px;
    cursor: pointer;
`;

type Props = {
    onChange: (version1: number, version2: number) => void;
    maxVersion: number;
};

export default function CustomPagination({ onChange, maxVersion }: Props) {
    const [version1, setVersion1] = useState(maxVersion || 1); // current version - first dropdown selected
    const [version2, setVersion2] = useState(maxVersion ? maxVersion - 1 : 0); // past version comparing with current - second dropdown

    const onNextClick = () => {
        setVersion1((v) => v - 1);
        setVersion2(version1 - 2);
        onChange(version1 - 1, version1 - 2);
    };
    const onPrevClick = () => {
        setVersion1((v) => v + 1);
        setVersion2(version1);
        onChange(version1 + 1, version1);
    };
    const onVersion1Click = ({ key }) => {
        const newVersion1 = parseInt(key, 10);
        setVersion1(newVersion1);
        if (version2 >= newVersion1) {
            setVersion2(newVersion1 - 1);
            onChange(newVersion1, newVersion1 - 1);
            return;
        }
        onChange(newVersion1, version2);
    };
    const onVersion2Click = ({ key }) => {
        setVersion2(parseInt(key, 10));
        onChange(version1, parseInt(key, 10));
    };

    const items1 = [...Array(maxVersion)].map((_, i) => {
        // eslint-disable-next-line react/no-array-index-key
        return {
            key: maxVersion - i,
            label: (
                <MenuItemStyle onClick={() => onVersion1Click({ key: maxVersion - i })}>
                    <Typography.Text>{i === 0 ? 'latest' : `version ${maxVersion + 1 - i}`}</Typography.Text>
                </MenuItemStyle>
            ),
        };
    });

    const items2 = [...Array(version1)].map((_, i) => {
        // eslint-disable-next-line react/no-array-index-key
        return {
            key: version1 - i - 1,
            label: (
                <MenuItemStyle onClick={() => onVersion2Click({ key: version1 - i - 1 })}>
                    <Typography.Text>{`version ${version1 - i}`}</Typography.Text>
                </MenuItemStyle>
            ),
        };
    });

    return (
        <CustomPaginationContainer>
            <NavButton
                size="small"
                type="text"
                icon={<LeftOutlined />}
                onClick={onPrevClick}
                disabled={version1 >= maxVersion}
            />
            <DescriptionText>Comparing</DescriptionText>
            <Dropdown menu={{ items: items1 }} trigger={['click']}>
                <VersionText strong type="success">
                    {version1 === maxVersion ? 'latest' : `version ${version1 + 1}`}
                </VersionText>
            </Dropdown>
            <DescriptionText>to</DescriptionText>
            <Dropdown menu={{ items: items2 }} trigger={['click']}>
                <VersionRightText strong type="success">{`version ${version2 + 1}`}</VersionRightText>
            </Dropdown>
            <NavButton
                size="small"
                type="text"
                icon={<RightOutlined />}
                onClick={onNextClick}
                disabled={version1 <= 1}
            />
        </CustomPaginationContainer>
    );
}
