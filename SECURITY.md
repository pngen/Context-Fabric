# Security

## Reporting a vulnerability

Context Fabric is infrastructure exposed to network input. If you believe you
have found a security vulnerability, please report it privately to the project
maintainers rather than opening a public issue.

Provide, if possible:

- A description of the vulnerability and its impact.
- The affected version and configuration.
- Steps to reproduce (code, commands, or a proof of concept).
- Any suggested fix.

Please do not include secrets, private keys, or real customer data in the
report.

## Trust model

Context Fabric v1 operates under an explicit **trusted-cluster network** model:

- The control and data planes have no authentication or encryption.
- The metadata coordinator is a single configured authority; it is not
  Byzantine-tolerant.
- Persisted data is checksummed but not encrypted at rest.
- Telemetry is local only and contains no secrets by default.

Deploy Context Fabric only on networks whose participants are trusted. Do not
expose the coordinator or node ports to untrusted networks.

## Guarantees we do maintain

- Strict frame parsing: length bounds, integer-overflow checks, mandatory
  checksums, and rejection of malformed input.
- Filesystem path containment for node stores: identities are hex-encoded and
  cannot be used for path traversal.
- Bounded queues, bounded thread pools, and operation timeouts.
- Corrupt or tampered persistent records are detected and quarantined, never
  silently accepted.

## Future work

Authenticated and encrypted transports are a designed extension point (see
`ROADMAP.md`). Until then, the trust model above applies.