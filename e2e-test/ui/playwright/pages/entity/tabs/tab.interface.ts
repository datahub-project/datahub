/**
 * Tab interface — all entity tab objects must implement open() so TypeScript
 * guarantees consistency across tab implementations.
 */
export interface Tab {
  open(): Promise<void>;
}
