const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const wsId = process.argv[2];
const nbId = process.argv[3];
const contentPath = process.argv[4];
const logicalId = process.argv[5] || nbId;
const displayName = process.argv[6] || 'notebook';

const b64 = (s) => Buffer.from(s, 'utf8').toString('base64');
const token = () =>
  JSON.parse(execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', { encoding: 'utf8' }))
    .accessToken;

(async () => {
  const content = fs.readFileSync(contentPath, 'utf8');
  const platform = JSON.stringify(
    {
      $schema: 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json',
      metadata: { type: 'Notebook', displayName },
      config: { version: '2.0', logicalId },
    },
    null,
    2
  );
  const t = token();
  const body = {
    definition: {
      format: 'fabricGitSource',
      parts: [
        { path: 'notebook-content.py', payload: b64(content), payloadType: 'InlineBase64' },
        { path: '.platform', payload: b64(platform), payloadType: 'InlineBase64' },
      ],
    },
  };
  const r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/updateDefinition`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`updateDefinition ${r.status} ${await r.text()}`);
  console.log('Fabric notebook updated:', displayName);
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
