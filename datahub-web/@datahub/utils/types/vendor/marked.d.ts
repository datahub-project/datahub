declare module 'marked' {
  // All marked options available to use in the library
  // See documentation to learn more about options:
  // https://marked.js.org/#/USING_ADVANCED.md#options
  interface IMarkedOptions {
    baseUrl?: string;
    breaks?: boolean;
    gfm?: boolean;
    headerIds?: boolean;
    headerPrefix?: boolean;
    highlight?: (code: string, lang: ILang, callback?: Callback) => void;
    langPrefix?: string;
    mangle?: boolean;
    pedantic?: boolean;
    renderer?: IMarkedRenderer;
    sanitize?: boolean;
    sanitizer?: () => void;
    silent?: boolean;
    smartLists?: boolean;
    smartypants?: boolean;
    tables: boolean;
    xhtml?: boolean;
  }

  interface ILang {
    lang: string;
    format: string;
  }

  // Interface used to override Marked's default renderer
  interface IMarkedRenderer {
    link: (href: string, title: string, text: string) => string;
    paragraph: (text: string) => string;
  }

  type Callback = (err: Error, result: string) => void;

  type IMarkedCreate = (param: string) => { htmlSafe: () => string };

  type Marked = IMarkedCreate & {
    Renderer: { new (): IMarkedRenderer };
    setOptions: (options: Partial<IMarkedOptions>) => void;
  };

  const marked: Marked;

  export default marked;
}
