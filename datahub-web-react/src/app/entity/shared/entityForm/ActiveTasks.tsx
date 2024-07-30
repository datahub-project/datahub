import React, { useState } from 'react';
import styled from 'styled-components';
import { Icon } from '@components';
import { pluralize } from '@src/app/shared/textUtil';
import { Divider } from 'antd';
import { useEntityFormContext } from './EntityFormContext';
import Task from './Task';

const ActiveTasksWrapper = styled.div`
    position: absolute;
    bottom: 0px;
    right: 20px;
    background-color: white;
    width: 400px;
    border: 1px solid #ebecf0;
    border-radius: 12px 12px 0px 0px;
    overflow: hidden;
`;

const Header = styled.div`
    font-size: 20px;
    background-color: #323a5d;
    padding: 12px;
    color: white;
    display: flex;
    align-items: center;
`;

const StyledIcon = styled(Icon)`
    margin-right: 6px;
`;

const ExpandCollapse = styled.div`
    margin-left: auto;
    cursor: pointer;
`;

const TasksContent = styled.div`
    padding: 16px;
    max-height: 230px;
    overflow: auto;
`;

const ActiveTaskCount = styled.div`
    margin-bottom: 8px;
    color: #81879f;
    font-size: 12px;
`;

const StyledDivider = styled(Divider)`
    margin: 0;
`;

export default function ActiveTasks() {
    const {
        submission: { activeTasks, completeTasks },
    } = useEntityFormContext();
    const [isExpanded, setIsExpanded] = useState(true);

    if (!activeTasks.length && !completeTasks.length) {
        return null;
    }

    const numActiveTasks = activeTasks.length;

    return (
        <ActiveTasksWrapper>
            <Header>
                <StyledIcon icon="TaskAlt" />
                Active Tasks
                <ExpandCollapse onClick={() => setIsExpanded(!isExpanded)}>{isExpanded ? '-' : '+'}</ExpandCollapse>
            </Header>
            {isExpanded && (
                <TasksContent>
                    {!!numActiveTasks && (
                        <ActiveTaskCount>
                            Running {numActiveTasks} active {pluralize(numActiveTasks, 'task')}
                        </ActiveTaskCount>
                    )}
                    <div>
                        {activeTasks.map((t, index) => (
                            <>
                                <Task task={t} isComplete={false} />
                                {(!!completeTasks.length || index !== numActiveTasks - 1) && <StyledDivider />}
                            </>
                        ))}
                        {completeTasks.map((t, index) => (
                            <>
                                <Task task={t} isComplete />
                                {index !== completeTasks.length - 1 && <StyledDivider />}
                            </>
                        ))}
                    </div>
                </TasksContent>
            )}
        </ActiveTasksWrapper>
    );
}
