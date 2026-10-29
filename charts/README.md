# Helm charts

`flink-lab` is the umbrella chart. It has two local dependencies:
- `session-mode`
- `application-mode`

From `charts/flink-lab`, run `helm dependency update` before lint/template. This creates `charts/` dependency archives from the local subcharts. Validate with:
- `helm lint . -f values-session.yaml`
- `helm template flink-lab . -f values-session.yaml`
- After approval: `helm lint . -f values-application.yaml`

Do not assume the chart is deployable until the Flink 2.2.0 command, image, connectors, network allow rules and actual cluster requirements are verified.
