declare module 'ember-concurrency' {
  export function timeout(delay: number): Promise<void>;
  import ComputedProperty from '@ember/object/computed';
  import RSVP from 'rsvp';

  export enum TaskInstanceState {
    Dropped = 'dropped',
    Canceled = 'canceled',
    Finished = 'finished',
    Running = 'running',
    Waiting = 'waiting'
  }

  export interface TaskProperty<T> extends ComputedProperty<T> {
    cancelOn(eventNames: string[]): this;
    debug(): this;
    drop(): this;
    enqueue(): this;
    group(groupPath: string): this;
    keepLatest(): this;
    maxConcurrency(n: number): this;
    on(eventNames: string[]): this;
    restartable(): this;
  }

  export interface TaskInstance<T> extends PromiseLike<T> {
    readonly error?: any;
    readonly hasStarted: ComputedProperty<boolean>;
    readonly isCanceled: ComputedProperty<boolean>;
    readonly isDropped: ComputedProperty<boolean>;
    readonly isError: ComputedProperty<boolean>;
    readonly isFinished: ComputedProperty<boolean>;
    readonly isRunning: ComputedProperty<boolean>;
    readonly isSuccessful: ComputedProperty<boolean>;
    readonly state: ComputedProperty<TaskInstanceState>;
    readonly value?: T;
    cancel(): void;
    catch: () => RSVP.Promise<any>;
    finally: () => RSVP.Promise<any>;
    then<TResult1 = T, TResult2 = never>(
      onfulfilled?: ((value: T) => TResult1 | RSVP.Promise<TResult1>) | undefined | null,
      onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | undefined | null
    ): RSVP.Promise<TResult1 | TResult2>;
  }

  export enum TaskState {
    Running = 'running',
    Queued = 'queued',
    Idle = 'idle'
  }

  type Task<T, P> = TaskProperty<T> &
    ComputedProperty<{ perform: P }> & {
      readonly isIdle: boolean;
      readonly isQueued: boolean;
      readonly isRunning: boolean;
      readonly last?: TaskInstance<T>;
      readonly lastCanceled?: TaskInstance<T>;
      readonly lastComplete?: TaskInstance<T>;
      readonly lastErrored?: TaskInstance<T>;
      readonly lastIncomplete?: TaskInstance<T>;
      readonly lastPerformed?: TaskInstance<T>;
      readonly lastRunning?: TaskInstance<T>;
      readonly lastSuccessful?: TaskInstance<T>;
      readonly performCount: number;
      readonly state: TaskState;
      cancelAll(): void;
    };

  export function task<T, A>(generatorFn: (a: A) => Iterator<T>): Task<T, (a?: A) => TaskInstance<T>>;

  export function task<T, A1, A2>(
    generatorFn: (a1: A1, a2: A2) => Iterator<T>
  ): Task<T, (a1: A1, a2: A2) => TaskInstance<T>>;

  export function task<T, A1, A2, A3>(
    generatorFn: (a1: A1, a2: A2, a3: A3) => Iterator<T>
  ): Task<T, (a1: A1, a2: A2, a3: A3) => TaskInstance<T>>;

  export function task<T, A1, A2, A3, A4>(
    generatorFn: (a1: A1, a2: A2, a3: A3, a4: A4) => Iterator<T>
  ): Task<T, (a1: A1, a2: A2, a3: A3, a4: A4) => TaskInstance<T>>;

  export function task<T, A1, A2, A3, A4, A5>(
    generatorFn: (a1: A1, a2: A2, a3: A3, a4: A4, a5: A5) => Iterator<T>
  ): Task<T, (a1: A1, a2: A2, a3: A3, a4: A4, a5: A5) => TaskInstance<T>>;

  export function task<T, A1, A2, A3, A4, A5, A6>(
    generatorFn: (a1: A1, a2: A2, a3: A3, a4: A4, a6: A6) => Iterator<T>
  ): Task<T, (a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6) => TaskInstance<T>>;
}
