const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const wsId = '2c3ac24f-d00f-474f-971a-63714de64268';
const nbId = '5722c626-8718-4b15-9fd3-c6ae6ff933cf';
const nbName = 'bronze_netsuite_batch_02';
const contentPath = path.join(
  process.env.USERPROFILE,
  '.cursor/NFF/Inputs/TIX-16063/notebooks/bronze_netsuite_incremental_load_batch_02.Notebook/notebook-content.py'
);
const content = fs.readFileSync(contentPath, 'utf8');

const token = () =>
  JSON.parse(execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', { encoding: 'utf8' }))
    .accessToken;
const b64 = (s) => Buffer.from(s, 'utf8').toString('base64');

const platform = JSON.stringify(
  {
    $schema: 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json',
    metadata: {
      type: 'Notebook',
      displayName: nbName,
      description: 'Ingesting Netsuite tables requiring incremental load',
    },
    config: { version: '2.0', logicalId: 'fb8e2626-627d-4551-a44c-ea561d345176' },
  },
  null,
  2
);

(async () => {
  const t = token();
  const r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/updateDefinition`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      definition: {
        format: 'fabricGitSource',
        parts: [
          { path: 'notebook-content.py', payload: b64(content), payloadType: 'InlineBase64' },
          { path: '.platform', payload: b64(platform), payloadType: 'InlineBase64' },
        ],
      },
    }),
  });
  if (!r.ok) throw new Error(`update ${r.status} ${await r.text()}`);
  console.log('DEPLOYED', nbName, nbId);
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
