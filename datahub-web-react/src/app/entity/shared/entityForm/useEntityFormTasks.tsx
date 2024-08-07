import {
    GetTestResultsSummaryQuery,
    GetTestResultSummariesQuery,
    useGetTestResultsSummaryLazyQuery,
    useGetTestResultSummariesLazyQuery,
} from '@src/graphql/test.generated';
import { AndFilterInput, Test } from '@src/types.generated';
import moment from 'moment';
import { useEffect, useState } from 'react';

export const BULK_VERIFY_ID = 'bulkVerify';
const TASK_TO_ID_MAP_KEY = 'taskToIdMap';
const ACTIVE_TASKS_KEY = 'activeTasks';
const LOCAL_STORAGE_TIMEOUT_MINS = 60;

export function useEntityFormTasks(formUrn: string) {
    const localStorageTasksKey = `${formUrn}-${ACTIVE_TASKS_KEY}`;
    const [activeTasks, setActiveTasks] = useState<Test[]>([]);
    const [completeTasks, setCompleteTasks] = useState<Test[]>([]);

    function handleFetchedTask(task: Test) {
        if (task?.results.lastRunTimestampMillis) {
            // task is complete
            const taskUrn = task.urn;
            const lastRunTime = task?.results.lastRunTimestampMillis;
            if (moment(lastRunTime).add(LOCAL_STORAGE_TIMEOUT_MINS, 'minutes') < moment()) {
                removeFromLocalStorage(taskUrn);
            } else {
                // add tasks within timeout window to complete tasks
                setCompleteTasks((completed) => [task as Test, ...completed]);
            }
            setActiveTasks((active) => active.filter((t) => t.urn !== taskUrn));
        } else if (task) {
            if (!activeTasks.find((t) => t.urn === task?.urn)) {
                setActiveTasks([task as Test, ...activeTasks]);
            }
            // eslint-disable-next-line @typescript-eslint/no-use-before-define
            setTimeout(() => fetchTask({ variables: { urn: task?.urn || '' } }), 3000);
        }
    }

    const [fetchTask] = useGetTestResultsSummaryLazyQuery({
        onCompleted: (data: GetTestResultsSummaryQuery) => {
            if (data.test) {
                handleFetchedTask(data.test as Test);
            }
        },
    });

    const [fetchTasks] = useGetTestResultSummariesLazyQuery({
        onCompleted: (data: GetTestResultSummariesQuery) => {
            data.listTests?.tests.forEach((test) => handleFetchedTask(test as Test));
        },
    });

    function removeFromLocalStorage(taskUrn: string) {
        const tasksInLocalStorage: string[] = JSON.parse(localStorage.getItem(localStorageTasksKey) || '[]');
        localStorage.setItem(localStorageTasksKey, JSON.stringify(tasksInLocalStorage.filter((t) => t !== taskUrn)));
    }

    /*
     * After batch submitting, add task to localStorage as an active task and with its associated promptId.
     * Then start fetching the task every 3 seconds until it's complete
     */
    function handleAsyncBatchSubmit(taskUrn: string, promptId: string) {
        const tasksInLocalStorage: string[] = JSON.parse(localStorage.getItem(localStorageTasksKey) || '[]');
        localStorage.setItem(localStorageTasksKey, JSON.stringify([...tasksInLocalStorage, taskUrn]));
        fetchTask({ variables: { urn: taskUrn } });

        // add to tasksToId map
        const tasksToIdMap: { [key: string]: string } = JSON.parse(localStorage.getItem(TASK_TO_ID_MAP_KEY) || '{}');
        tasksToIdMap[taskUrn] = promptId;
        localStorage.setItem(TASK_TO_ID_MAP_KEY, JSON.stringify(tasksToIdMap));
    }

    /*
     * On page load, get tasks from localStorage to display to the user.
     */
    useEffect(() => {
        const tasksInLocalStorage: string[] = JSON.parse(localStorage.getItem(localStorageTasksKey) || '[]');
        const urnsFilter = { field: 'urn', values: tasksInLocalStorage };
        const orFilters: AndFilterInput[] = [{ and: [urnsFilter] }];
        fetchTasks({ variables: { input: { orFilters, start: 0, count: 50 } } });
    }, [fetchTasks, localStorageTasksKey]);

    return { activeTasks, completeTasks, handleAsyncBatchSubmit };
}

export function getAssociatedPromptId(taskId: string): string | undefined {
    const tasksToIdMap: { [key: string]: string } = JSON.parse(localStorage.getItem(TASK_TO_ID_MAP_KEY) || '{}');
    return tasksToIdMap[taskId];
}
