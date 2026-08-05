# NeKiro A2A Transport RepoWiki

这里是可复用 A2A HTTP、JSON-RPC 和 SSE transport mechanics 的中英文
RepoWiki 入口。transport README 是 canonical source，MkDocs 在 CI 中生成站点。

## 从这里开始

- [源文档](source-docs/index.md)：范围、兼容性、使用方式和验证。
- [GitHub 仓库](https://github.com/NeKiro-project/nekiro-a2a-transport-go)：源码、Issue 和 Release。
- [Core RepoWiki](https://nekiro-project.github.io/NeKiro/zh/)：平台契约与架构。

该模块不负责 discovery、endpoint selection、credentials、authorization、
retry、fallback、持久化、Ledger 或 Agent Runtime。
