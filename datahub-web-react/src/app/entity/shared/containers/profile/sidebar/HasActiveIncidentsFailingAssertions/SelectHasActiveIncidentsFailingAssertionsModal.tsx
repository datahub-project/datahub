import { Button, Form, Modal } from 'antd';
import React, { useState } from 'react';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';
import BooleanMoreFilter from '../../../../../../search/filters/render/shared/BooleanMoreFilter';

type Props = {
    onCloseModal: () => void;
    titleOverride?: string;
    filter?: any;
    onOk: (result: string[]) => void;
    dropdownTitle: string;
    dropdownOptions: string;
    icon: React.ReactNode;
};

export const SelectHasActiveIncidentsFailingAssertionsModal = ({
    onCloseModal,
    titleOverride,
    filter,
    onOk,
    dropdownTitle,
    dropdownOptions,
    icon,
}: Props) => {
    const [filterValue, setFilterValue] = useState(false);

    const onModalClose = () => {
        onCloseModal();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setIncidentsAssertionsButton',
    });

    const toggleFilter = (selected) => {
        if (selected) {
            setFilterValue(true);
        } else {
            setFilterValue(false);
        }
    };

    let aggregateCount;
    if (filter) {
        aggregateCount = filter.aggregations.find((agg) => agg.value === 'true')?.count;
    }

    const handleOk = () => {
        if (onOk) {
            onOk([`${filterValue}`]);
        }
    };

    return (
        <Modal
            title={titleOverride}
            visible
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        Cancel
                    </Button>
                    <Button id="setIncidentsAssertionsButton" disabled={false} onClick={handleOk}>
                        Add
                    </Button>
                </>
            }
        >
            <Form component={false}>
                <Form.Item>
                    <BooleanMoreFilter
                        icon={icon}
                        title={dropdownTitle}
                        option={dropdownOptions}
                        initialSelected={false}
                        onUpdate={toggleFilter}
                        count={aggregateCount}
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
};
