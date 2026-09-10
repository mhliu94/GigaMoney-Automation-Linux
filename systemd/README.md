# Generate a service for this installation

The former template assumed a specific checkout location. Generate the service
from the account's configuration and the Python environment being used:

```bash
.venv/bin/gigamoney --config config/gigamoney.config.json render-service
```

This writes `<paths.workDir>/systemd/<service.name>.service` without installing or
starting it. Copy the generated file to `~/.config/systemd/user/`, then run
`systemctl --user daemon-reload`. Enable/start it when ready to consume commands.
See [deployment instructions](../docs/deployment.md) for the complete sequence.
