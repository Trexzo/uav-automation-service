# Configuration profiles

`profile.json` is intentionally not committed. Copy `profile.example.json` to
`profile.json` and replace the sample matching rules with your own deployment
configuration.

A profile defines route names, environment-variable references, message rules,
timed rules, counter groups, named lists, rotations, and optional historical
counters. Keep private operational terms in the untracked `profile.json` file.
