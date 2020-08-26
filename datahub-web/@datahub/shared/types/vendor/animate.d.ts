declare module '@f/animate' {
  export default function animate<T>(start: T, end: T, fn: (props: T, step: number) => void): void;
}
