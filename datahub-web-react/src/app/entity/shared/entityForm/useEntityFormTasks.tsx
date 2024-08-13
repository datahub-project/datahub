import {
    GetTestResultsSummaryQuery,
    GetTestResultSummariesQuery,
    useGetTestResultsSummaryLazyQuery,
    useGetTestResultSummariesLazyQuery,
} from '@src/graphql/test.generated';
import { AndFilterInput, Test } from '@src/types.generated';
import moment from 'moment';
import { useEffect, useRef, useState } from 'react';

export const BULK_VERIFY_ID = 'bulkVerify';
const TASK_TO_ID_MAP_KEY = 'taskToIdMap';
const ACTIVE_TASKS_KEY = 'activeTasks';
const LOCAL_STORAGE_TIMEOUT_MINS = 60;

export function useEntityFormTasks(formUrn: string) {
    const localStorageTasksKey = `${formUrn}-${ACTIVE_TASKS_KEY}`;
    const [activeTasks, setActiveTasks] = useState<Test[]>([]);
    const [completeTasks, setCompleteTasks] = useState<Test[]>([]);
    const [isFetchingActiveTasks, setIsFetchingActiveTasks] = useState(false);
    const activeTasksRef = useRef(activeTasks);
    activeTasksRef.current = activeTasks;

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
     * Fetch the list of active tasks every 3 seconds until there are no more active tasks
     */
    useEffect(() => {
        if (activeTasks.length && !isFetchingActiveTasks) {
            setIsFetchingActiveTasks(true);
            const interval = setInterval(() => {
                if (activeTasksRef.current.length) {
                    const orFilters: AndFilterInput[] = [
                        { and: [{ field: 'urn', values: activeTasksRef.current.map((t) => t.urn) }] },
                    ];
                    fetchTasks({ variables: { input: { orFilters, start: 0, count: 50 } } });
                } else {
                    clearInterval(interval);
                    setIsFetchingActiveTasks(false);
                }
            }, 3000);
        }
    }, [activeTasks, isFetchingActiveTasks, fetchTasks]);

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
