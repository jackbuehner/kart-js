import type { Config } from 'release-it';

export default {
  git: {
    commit: true,
    tag: true,
    push: true,
    commitMessage: 'chore: release v${version}',
    tagName: 'v${version}',
  },
  npm: {
    publish: true,
  },
} satisfies Config;
