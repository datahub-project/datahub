export interface IPackageJson {
  name: string;
  dependencies: Record<string, string>;
  devDependencies: Record<string, string>;
  peerDependencies?: Record<string, string>;
  transitiveDependencies?: Record<string, string>;
}
