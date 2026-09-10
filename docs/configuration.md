# Configuration reference

Generated from the supported defaults; the complete JSON template is [gigamoney.config.example.json](../config/gigamoney.config.example.json). Unknown field names and invalid types are rejected. Keep account configurations and app profiles private.

Paths are project-relative unless absolute. Empty derived-path settings are explained below. CLI overrides such as `--config` and `--verbose` are optional; persistent operator values belong in JSON.

## paths

| Field | Default | Meaning |
|---|---|---|
| `paths.workDir` | `"work"` | Persistent PID files, locks, trading state, API journal and default session/proxy directories. Use a separate directory per account. |
| `paths.logDir` | `"logs"` | Default runner, monitor and emulator log directory. |

## logging

| Field | Default | Meaning |
|---|---|---|
| `logging.level` | `"INFO"` | CLI/API log verbosity; --verbose overrides this with DEBUG. |

## gigamoney

| Field | Default | Meaning |
|---|---|---|
| `gigamoney.backend` | `"android"` | android or api. API mode needs a verified account-bound session. |
| `gigamoney.package` | `"lb.whale.hkwinner.android"` | Android package used for device checks, launching and session extraction. |
| `gigamoney.tradePassword` | `""` | Private trading password for Android automation and session import; not the login password or phone code. |
| `gigamoney.accountId` | `""` | Required logical account_id used by Kafka/control UI; must match the bound session. |
| `gigamoney.accountNumId` | `null` | Optional numeric account identifier included in holdings snapshots. |
| `gigamoney.tradingEnabledDefault` | `true` | Initial trading state when no persisted state exists. Existing persisted state takes precedence. |
| `gigamoney.foregroundLaunchTimeoutSeconds` | `15` | Time allowed for Android foreground launch. |

## adb

| Field | Default | Meaning |
|---|---|---|
| `adb.path` | `""` | ADB executable; empty discovers SDK environment variables, the user SDK, or PATH. |
| `adb.serial` | `""` | Device serial, e.g. emulator-5554. Required for credential extraction; Android automation otherwise requires exactly one ready device. |
| `adb.expectedResolution` | `"1080x2424"` | Recorded Android layout resolution; changing this alone does not adapt the UI coordinates. |
| `adb.expectedDensity` | `420` | Recorded Android display density. |
| `adb.commandTimeoutSeconds` | `30` | Default ADB command and enrollment extraction timeout. |

## ocr

| Field | Default | Meaning |
|---|---|---|
| `ocr.command` | `"tesseract"` | Tesseract executable. |
| `ocr.language` | `"eng"` | Installed Tesseract language. |

## api

| Field | Default | Meaning |
|---|---|---|
| `api.sessionPath` | `""` | Private generated session file. Empty uses <workDir>/api/session.json. |
| `api.accountAaid` | `""` | Broker account identifier from api-bootstrap inspect; distinct from logical accountId. |
| `api.orgId` | `""` | Broker organization identifier from the inspected account route. |
| `api.accountChannel` | `""` | Broker account channel from the inspected account route. |
| `api.allowedSymbols` | `["NVDA", "BIYA", "AEHL", "TANH"]` | Explicit order-symbol allowlist. Cancel-all targets the configured account across symbols. |
| `api.maxOrderQuantity` | `null` | Maximum shares per submission; null disables this cap. Broker lot-size and no-short checks remain. |
| `api.maxOrders` | `null` | Maximum persistent dispatch count across this account journal, including rejected dispatches; null disables the cap. |
| `api.maxOrderPages` | `100` | Maximum pages when listing orders; incomplete lists fail. |
| `api.fokWaitSeconds` | `2` | Delay before checking/canceling the unfilled remainder of synthetic FOK. |
| `api.positionCost` | `"diluted"` | diluted or average cost representation for holdings. |
| `api.extendedHoursMarketOrders` | `true` | Convert market commands to Kafka-priced limit orders in premarket/after-hours. If false, market commands outside regular hours fail before submission. |
| `api.marketStatusMaxAgeSeconds` | `30` | Maximum age of the broker's non-delayed US trading-session status. |
| `api.requestTimeoutSeconds` | `15` | HTTP request timeout. |
| `api.maxResponseBytes` | `4194304` | Maximum accepted HTTP response body size. |
| `api.refreshMarginSeconds` | `60` | Renew tokens when this many seconds or fewer remain. |
| `api.sessionLockTimeoutSeconds` | `30` | Wait for the shared credential-file lock. |
| `api.readAttempts` | `2` | Maximum attempts for retryable transport/server read failures. Financial writes are not blindly retried. |
| `api.readRetryDelaySeconds` | `0.25` | Delay between retryable reads. |
| `api.cancelPollDelaysSeconds` | `[0, 1, 1, 2]` | Delays before each cancellation confirmation read. |
| `api.bootstrap.capturePath` | `""` | Local flows.mitm file containing this account’s verified authentication and account reads. |
| `api.bootstrap.appProfilePath` | `"config/api-app-profile.json"` | Private app-build signing profile; export from the reviewed installation and transfer privately. |
| `api.bootstrap.maxCaptureAgeSeconds` | `1800` | Maximum age of enrollment capture evidence. |

## proxy

| Field | Default | Meaning |
|---|---|---|
| `proxy.stateDir` | `""` | Private capture, CA, trust and state directory. Empty uses <workDir>/proxy. |
| `proxy.pythonPath` | `""` | Python environment with proxy/bootstrap dependencies. Empty uses <proxyDir>/venv/bin/python; mitmdump is its sibling. |
| `proxy.apiPort` | `18080` | Loopback reverse-proxy port; use a distinct unprivileged port for each simultaneous deployment. |
| `proxy.startupTimeoutSeconds` | `15` | Manager wait for proxy startup. |
| `proxy.evidenceMaxAgeSeconds` | `300` | Maximum age of successful account-response evidence for capture-ready status. |
| `proxy.boot.sourceImage` | `""` | Original reviewed Android SDK image directory for the boot builder. |
| `proxy.boot.magiskApk` | `""` | Local pinned Magisk APK used by the boot builder. |
| `proxy.boot.outputDirectory` | `""` | Output directory for the separate boot image. Empty uses <proxyDir>/image; existing output is not overwritten. |

## androidBuild

| Field | Default | Meaning |
|---|---|---|
| `androidBuild.sdkRoot` | `""` | SDK root; empty uses ANDROID_SDK_ROOT, ANDROID_HOME or the user SDK. |
| `androidBuild.platform` | `""` | Installed platform directory name; empty selects the highest installed version. |
| `androidBuild.buildToolsVersion` | `""` | Installed build-tools directory name; empty selects the highest installed version. |
| `androidBuild.javaHome` | `""` | JDK directory; empty uses JAVA_HOME, javac on PATH or the user’s Android Studio JBR. |
| `androidBuild.junitJar` | `""` | JUnit 4 jar; empty discovers JUNIT_JAR, the OS package or Android Studio. |
| `androidBuild.outputJar` | `"android/gigamoney-zero-idle-dump.jar"` | Generated hierarchy helper consumed by the Android backend. |

## service

| Field | Default | Meaning |
|---|---|---|
| `service.name` | `"gigamoney-runner"` | Generated systemd unit basename; use distinct names for distinct deployments. |
| `service.restartSeconds` | `10` | Systemd delay after a process failure. Authentication-required stops exit cleanly for manual recovery. |

## automation

| Field | Default | Meaning |
|---|---|---|
| `automation.lockTimeoutSeconds` | `300` | Wait for exclusive workflow access. |
| `automation.maxHomeBacks` | `3` | Maximum Back operations while locating Android home. |
| `automation.maxWatchlistScrolls` | `8` | Maximum Android watchlist scrolls while finding a symbol. |
| `automation.maxPositionScrolls` | `8` | Maximum Android holdings scrolls; incomplete holdings fail. |
| `automation.maxCancelCount` | `50` | Maximum orders processed by one cancel-all workflow; remaining open orders cause failure. |

## kafka

| Field | Default | Meaning |
|---|---|---|
| `kafka.bootstrapServers` | `""` | Required broker connection string. |
| `kafka.commandTopic` | `"trading-commands"` | Topic carrying order/control commands. |
| `kafka.accountDetailsTopic` | `"account-details"` | Topic receiving holdings snapshots. |
| `kafka.consumerGroupId` | `""` | Required group unique per account; different accounts sharing a group can lose commands through account filtering. |
| `kafka.priceBooksTopic` | `"price-books"` | Order-book snapshots used to price API market commands. Uses the same Kafka connection/security settings and a separate reader that never commits. |
| `kafka.priceBookMaxAgeSeconds` | `null` | Optional maximum age of the source book and selected side's nanosecond update_time. Null accepts unchanged books without an age cutoff; future timestamps remain invalid. |
| `kafka.priceBookLookbackSeconds` | `3600` | Initial history window for locating a symbol's latest book; a configured max age takes precedence. Later requests reuse the book and scan new offsets. |
| `kafka.priceBookTimeoutSeconds` | `30` | Total deadline for finding/catching up to a price book; missing data fails without submitting. |
| `kafka.priceBookBatchSize` | `500` | Messages per read and size of the backwards search window for a single-partition topic. |
| `kafka.autoOffsetReset` | `"latest"` | Behavior when the group has no committed offset; latest avoids consuming historical commands at first startup. |
| `kafka.pollTimeoutSeconds` | `1` | Consumer polling wait. |
| `kafka.maxPollIntervalMilliseconds` | `900000` | Maximum interval between polls; allow enough time for workflows. |
| `kafka.maxCommandAgeSeconds` | `300` | Reject commands older than this on receipt/redelivery; missing or invalid timestamps are rejected. |
| `kafka.commitFailedCommands` | `true` | Commit ordinary failed commands to avoid unsafe order replay. Verification before submission stops intake without committing. |
| `kafka.holdingsIntervalSeconds` | `30` | Interval from completion of each update, including startup/post-order updates and ordinary failed attempts. |
| `kafka.clientVersion` | `"2.15.0"` | Retained compatibility metadata setting; does not select or upgrade the installed Android app. |
| `kafka.sessionTimeoutMilliseconds` | `10000` | Kafka consumer session timeout. |
| `kafka.deliveryTimeoutSeconds` | `10` | Maximum wait for each holdings snapshot delivery. |
| `kafka.producerLingerMilliseconds` | `5` | Producer batching delay. |
| `kafka.clientConfig` | `{}` | Additional common librdkafka SSL/SASL/client options. Credentials stay private; core routing/commit settings cannot be overridden. |

## monitor

| Field | Default | Meaning |
|---|---|---|
| `monitor.serverId` | `""` | Display label; empty uses the Linux hostname. |
| `monitor.host` | `"127.0.0.1"` | HTTPS bind address. |
| `monitor.port` | `8787` | HTTPS port; privileges are required below 1024. |
| `monitor.redirectHttpPort` | `0` | Optional HTTP-to-HTTPS redirect listener; 0 disables it. |
| `monitor.publicHost` | `""` | Public hostname/IP for links and redirects; empty discovers an address. |
| `monitor.intervalSeconds` | `60` | Dashboard health polling interval; separate from Kafka holdings frequency. |
| `monitor.certFile` | `""` | HTTPS certificate file. |
| `monitor.keyFile` | `""` | Private HTTPS key file. |
| `monitor.adminPassword` | `""` | Required private command-center password. |
| `monitor.adminUsername` | `"admin"` | Command-center login username. |
| `monitor.sessionLifetimeSeconds` | `28800` | Authenticated dashboard cookie/session lifetime. |
| `monitor.loginWindowSeconds` | `300` | Window over which failed dashboard logins are counted. |
| `monitor.loginLockoutSeconds` | `900` | Per-client lockout duration after too many failed logins. |
| `monitor.maxLoginFailures` | `5` | Failures required for dashboard lockout. |
| `monitor.avd` | `""` | Android Virtual Device name; empty requires exactly one discoverable AVD. |
| `monitor.emulatorPath` | `""` | Emulator executable; empty discovers the SDK. |
| `monitor.emulatorArgs` | `["-netdelay", "none", "-netspeed", "full", "-no-snapshot-load"]` | Additional emulator arguments. Cold boot is enforced; shell expansion is not performed. |
| `monitor.emulatorBootTimeoutSeconds` | `300` | Maximum wait for Android boot readiness. |
| `monitor.display` | `""` | DISPLAY for emulator startup when not already provided by the monitor environment. |
| `monitor.runnerLog` | `""` | Explicit runner log path; empty uses <logDir>/runner.log. |
| `monitor.restart.enabled` | `true` | Enable the monitor’s scheduled restart process. |
| `monitor.restart.timezone` | `"America/New_York"` | IANA timezone for scheduled restart windows. |
| `monitor.restart.start` | `"20:01"` | Inclusive HH:MM window start; crossing midnight is supported. |
| `monitor.restart.end` | `"21:00"` | Inclusive HH:MM window end. |
| `monitor.restart.checkIntervalSeconds` | `30` | Scheduler polling interval. |
| `monitor.restart.minIntervalSeconds` | `3600` | Minimum elapsed time between recorded restart attempts. |
| `monitor.restart.runnerStopTimeoutSeconds` | `360` | Maximum wait for the runner to finish its current operation. |
| `monitor.restart.runnerPollSeconds` | `0.5` | Runner-stop polling interval. |
| `monitor.restart.emulatorStopSettleSeconds` | `60` | Delay after stopping Android before starting it again. |
| `monitor.restart.emulatorBootPollSeconds` | `5` | Android readiness polling interval. |
| `monitor.restart.foregroundMaxTries` | `5` | Maximum attempts to foreground GigaMoney after boot. |
| `monitor.restart.foregroundRetrySeconds` | `5` | Delay after foreground attempts. |

## Generated session and app profile

The private app profile has `version`, `api_secret`, and `engineConfigClass` fields. It is build-specific and contains no account session. `api-bootstrap export-profile` creates it from the reviewed installation. Its example deliberately contains no usable signing secret.

The generated private session contains `version`, `account_id`, `host`, `api_secret`, `headers`, `authorization`, `refresh_token`, `trade_token`, `trade_refresh_token`, `templates`, `trade_auth` and `updated_at`. `trade_auth` contains the verified `android_device_tag`, `type`, and password digest. The proxy manager adds/removes `capture_proxy`. These values are generated/imported and renewed by the tools, not manually edited operator settings. A runtime session cannot be reused for a different configured account.
