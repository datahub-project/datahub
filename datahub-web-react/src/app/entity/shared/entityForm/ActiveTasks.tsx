import React, { useState } from 'react';
import styled from 'styled-components';
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
    z-index: 100;
`;

const Header = styled.div`
    font-size: 16px;
    background-color: black;
    padding: 12px;
    color: white;
    display: flex;
    align-items: center;
`;

const ExpandCollapse = styled.div`
    margin-left: auto;
    cursor: pointer;
`;

const TasksContent = styled.div`
    padding: 8px 16px;
    max-height: 230px;
    overflow: auto;
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
    const allTasks = [...activeTasks, ...completeTasks];

    return (
        <ActiveTasksWrapper>
            <Header>
                Pending Responses
                <ExpandCollapse onClick={() => setIsExpanded(!isExpanded)}>{isExpanded ? '-' : '+'}</ExpandCollapse>
            </Header>
            {isExpanded && (
                <TasksContent>
                    <div>
                        {activeTasks.map((t, index) => (
                            <div key={t.urn}>
                                <Task task={t} isComplete={false} allTasks={allTasks} />
                                {(!!completeTasks.length || index !== numActiveTasks - 1) && <StyledDivider />}
                            </div>
                        ))}
                        {completeTasks.map((t, index) => (
                            <div key={t.urn}>
                                <Task task={t} isComplete allTasks={allTasks} />
                                {index !== completeTasks.length - 1 && <StyledDivider />}
                            </div>
                        ))}
                    </div>
                </TasksContent>
            )}
        </ActiveTasksWrapper>
    );
}
