import { Select } from 'antd';
import React, { ReactNode } from 'react';
import { StyledTag } from './styledComponents';

interface Props {
    onSelect: (asset: any) => void;
    onDeselect: (asset: any) => void;
    setInputOnSearch: React.Dispatch<React.SetStateAction<string>>;
    handleActorSearch: (text: string, isGroup: boolean) => void;
    options: JSX.Element[];
    isGroup: boolean;
}

const UserOrGroupSelect = ({ onSelect, onDeselect, setInputOnSearch, handleActorSearch, options, isGroup }: Props) => {
    const tagRender = ({ closable, label, onClose }: { closable: boolean; label: ReactNode; onClose: () => void }) => {
        return (
            <StyledTag
                onMouseDown={(event) => {
                    event.preventDefault();
                    event.stopPropagation();
                }}
                closable={closable}
                onClose={onClose}
            >
                {label}
            </StyledTag>
        );
    };

    return (
        <Select
            labelInValue
            autoFocus
            mode="multiple"
            placeholder={`Search for ${isGroup ? 'groups' : 'users'}...`}
            showSearch
            filterOption={false}
            defaultActiveFirstOption={false}
            onSelect={(asset: any) => onSelect(asset)}
            onDeselect={(asset: any) => onDeselect(asset)}
            onSearch={(value: string) => {
                handleActorSearch(value.trim(), isGroup);
                setInputOnSearch(value.trim());
            }}
            tagRender={tagRender}
            onBlur={() => setInputOnSearch('')}
            data-testid={`${isGroup ? 'group-select' : 'user-select'}`}
        >
            {options}
        </Select>
    );
};

export default UserOrGroupSelect;
