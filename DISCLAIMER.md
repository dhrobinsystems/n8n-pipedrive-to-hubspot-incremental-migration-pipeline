# ⚠️ Disclaimer & Liability Limitation

**Use at your own risk.** The code, workflows, and scripts in this repository (**n8n-pipedrive-to-hubspot-incremental-migration-pipeline**) are provided for educational and architectural demonstration purposes.

**No Liability:** The author and dhRobin Systems accept no responsibility or liability for any direct, indirect, incidental, or consequential damages that result from using, misusing, or deploying these scripts. That includes data loss or corruption, incomplete or duplicated migrations, records overwritten in HubSpot, incorrect owner, pipeline or deal-stage mappings, missing or wrong associations, quarantined records that are never migrated, API rate-limit or quota consequences, and business decisions taken on the basis of migrated data or validation reports.

**No Warranty:** All software, n8n JSON blueprints, JavaScript modules, field mappings and sample payloads are provided "AS IS", without warranty of any kind, express or implied. This includes, but is not limited to, warranties of merchantability, fitness for a particular purpose, and non-infringement. The default field mapping covers a subset of Pipedrive and HubSpot fields. Custom fields, notes, files, products, line items, email history, deletions and merges are not migrated unless you extend the workflow. Pipedrive and HubSpot APIs, limits and object models can change at any time.

**Production Testing Required:** Always test thoroughly in an isolated staging environment, such as a HubSpot developer test account or sandbox and a copy of your Pipedrive data, with `SANDBOX_MODE=true` and `DRY_RUN=true` enabled, before you connect live Pipedrive or HubSpot credentials or set `HUBSPOT_WRITES_ENABLED=true`. Review every dry-run validation report, take a HubSpot export or backup before the first live run, and treat every migrated record as data that a person must spot-check before it's relied on.

**Third-Party APIs & Compliance:** You alone are responsible for complying with the terms of service, rate limits, data privacy regulations (GDPR/CCPA), data-processing agreements, retention rules and security policies of Pipedrive, HubSpot, n8n and any connected services. That includes confirming a lawful basis for transferring personal data between systems, honouring consent, opt-out and deletion requests in both systems, limiting API token scopes, protecting n8n execution data that contains CRM records, and reviewing every automatically created or updated record.

The full disclaimer is also available in [DISCLAIMER.md](DISCLAIMER.md).

## License

Released under the MIT License. Copyright (c) 2026 DH Robin (dhRobin Systems). See [LICENSE](LICENSE).

---

Full text: DISCLAIMER.md · Licensed under the MIT License. License: MIT © 2026 dhRobin Systems · Provided as-is, see the disclaimer.
