# Umbrella chart

This umbrella chart coordinates the two deployment stages while keeping namespaces separate.

- Default: `sessionMode.enabled=true`, `applicationMode.enabled=false`.
- Stage 1: install/sync the umbrella release with Session Mode enabled.
- Stage 2: after the Session Mode baseline is accepted, enable Application Mode in a reviewed values file and sync it.
- Do not enable both against the same production Kafka consumer groups unless a controlled migration/shadow-consumption plan is in place.

## Version updates with Argo CD

Argo CD continuously reconciles the chart's desired state from Git. It does **not** automatically select a newer image/chart version merely because a new image tag exists. For deterministic updates:
1. Build and publish an immutable image tag (ideally commit SHA or release tag).
2. Update the image tag in a values file via a reviewed PR or image automation controller.
3. Merge the change; Argo CD detects Git changes and syncs according to the Application sync policy.
4. Keep Application Mode disabled until its migration gate is approved.

Do not use `latest` for production. Automatic image-tag discovery requires a separate tool such as Argo CD Image Updater or a GitHub Actions workflow that commits the new tag to Git; it is not implicit in Argo CD.
