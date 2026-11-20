# Security policy

## Supported versions

perago is pre-1.0. Security fixes go into the latest release only.

## Reporting a vulnerability

Please do not open a public issue. Report it privately through GitHub: **Security → Report a vulnerability** on the [repository page](https://github.com/LouisRiverstone/perago/security/advisories/new).

Include the perago version, the queue driver and stores you use, a minimal reproduction, and what an attacker could do with it. You will get an answer within a few days, and a fix or a clear decision after that.

## What counts

- A payload read from a queue that makes a worker run code other than a registered job's `Handle` (perago only builds jobs from factories registered with `Register`).
- Weaknesses in `ShouldBeEncrypted` payload encryption (AES-256-GCM with `Config.Key`).
- SQL injection in the database driver or stores (every value is a bound parameter; table names come from your configuration and are quoted).
- A command that runs destructive work in production without `--force` or the confirmation prompt.

Anyone who can write to your queue backend can enqueue registered jobs with arbitrary field values: treat queue access like database access, and validate job fields in `Handle`.
