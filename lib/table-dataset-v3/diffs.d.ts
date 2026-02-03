declare namespace KartDiff {
  namespace HexWkB {
    namespace v1 {
      export type Insert = {
        '++': string | Record<string, unknown>;
      };
      export type Delete = {
        '--': string | Record<string, unknown>;
      };
      export type Update = {
        '-'?: string | Record<string, unknown>;
        '+': string | Record<string, unknown>;
      };
      export type Change = Insert | Delete | Update;

      export type MetaChanges = {
        title?: Change;
        description?: Change;
        'schema.json'?: Change;
      };

      export type Diff = Record<
        string,
        {
          meta?: MetaChanges;
          feature?: Change[];
        }
      >;
    }
  }
}
