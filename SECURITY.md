# Security policy

This is a personal, local-first teaching project. It is meant to run on your own laptop, not to be exposed to a network.

## Supported versions

Only the `main` branch gets fixes. There are no releases with long-term support.

## Reporting a vulnerability

Please do not open a public issue for a security problem. Report it privately through
[GitHub private vulnerability reporting](https://github.com/santoshshinde2012/local-data-lakehouse/security/advisories/new)
and include the steps to reproduce, the affected file or service, and the impact you see.
If that form is not available, open an issue that only asks for a private contact. Leave the details out.

You can expect an acknowledgement within a week. The fix and the advisory are published together.

## Scope and known trade-offs

These are deliberate and documented, so they are not vulnerabilities on their own:

- **Sample credentials.** `.env.example` holds teaching-only credentials (`make env` copies it to `.env`), and `make airflow-up` generates the Airflow secrets on first run.
  Never reuse them anywhere else.
- **Docker socket.** Airflow never sees the Docker socket: it talks to the `docker-proxy` socket proxy, which only
  accepts the scheduler and allows a narrow set of API calls ([docs/airflow.md](docs/airflow.md)).
- **Local ports.** The UIs and APIs listen on localhost for a single user. Do not publish them to the internet.
- **Graph agent.** The MCP servers run locally over stdio with no authentication, and approving `.mcp.json` runs repo
  code. The tools are read-only, but that is not a sandbox: the OS sandbox is macOS-only. Read the warnings in
  [docs/graph/agent.md](docs/graph/agent.md) first.

Real issues include a secret committed to the repo, a container that runs as root when it should not, a way past the
socket proxy or the read-only tools, or a pinned image or dependency with a known critical CVE.
