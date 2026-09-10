# Configuration files

Both trading backends use the same main JSON file. Select `android` or `api` with
`gigamoney.backend`; account, Kafka, publication, and logging settings are shared.

| File | Purpose | In Git? |
| --- | --- | --- |
| `gigamoney.config.example.json` | Complete defaults for machine, account, Kafka, backend, proxy, and monitor settings. | Yes |
| `gigamoney.config.json` | This installation's private configuration. | No |
| `api-app-profile.example.json` | App profile format with an empty signing-secret placeholder. | Yes |
| `api-app-profile.json` | Private signing profile for the supported app build; required for API enrollment. | No |

Run `.venv/bin/gigamoney init-config` from the checkout to create the private main
file without overwriting an existing one. All non-example JSON files directly in
this directory are ignored by Git. Keep private files at mode `600`.

An API session is generated separately, at `work/api/session.json` by default.
It contains the account's tokens and device identity and is renewed by the API
client. The app profile contains signing material for the app build; it contains
no account session. Obtain the working profile through private deployment setup,
then enroll each account using its own verified emulator session.

For another configuration, pass `--config /absolute/path/account.json` before the
CLI subcommand, or set `GIGAMONEY_CONFIG`. Relative paths inside JSON resolve
against the checkout root. Give each account its own working directory, session,
and Kafka consumer group.

See the [deployment guide](../docs/deployment.md) for enrollment and startup, and
the [configuration reference](../docs/configuration.md) for every supported field.
