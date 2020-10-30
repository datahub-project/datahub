/**
 * Will execute one of the scripts in process
 */
const work = async function(): Promise<void> {
  const myArgs = process.argv.slice(2);
  const [command] = myArgs;
  const fn = (await import('./process/' + command)).default;
  await fn();
};
work();
