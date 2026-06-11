const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const wsId = '5a2907f0-782e-4dd3-80c2-35e22d411d08';
const contentPath = path.join(__dirname, 'TIX-30119_validate-content.py');
const content = fs.readFileSync(contentPath, 'utf8');

const token = () =>
  JSON.parse(execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', { encoding: 'utf8' }))
    .accessToken;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const b64 = (s) => Buffer.from(s, 'utf8').toString('base64');

(async () => {
  let t = token();
  let r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      displayName: 'TIX-30119_cs-reassign_validate',
      type: 'Notebook',
      description: 'TIX-30119 after validation',
    }),
  });
  if (!r.ok) throw new Error(`create ${r.status} ${await r.text()}`);
  const item = await r.json();
  const nbId = item.id;
  console.log('Notebook id:', nbId);

  const platform = JSON.stringify(
    {
      $schema: 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json',
      metadata: { type: 'Notebook', displayName: 'TIX-30119_cs-reassign_validate' },
      config: { version: '2.0', logicalId: nbId },
    },
    null,
    2
  );

  r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/updateDefinition`, {
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

  r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/jobs/RunNotebook/instances`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: '{}',
  });
  if (r.status !== 202) throw new Error(`run ${r.status} ${await r.text()}`);
  const instanceId = r.headers.get('x-ms-job-id');
  console.log('Job:', instanceId);

  const pollUrl = `https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items/${nbId}/jobs/instances/${instanceId}`;
  for (let i = 0; i < 60; i++) {
    await sleep(15000);
    t = token();
    const st = await fetch(pollUrl, { headers: { Authorization: `Bearer ${t}` } });
    const js = await st.json();
    console.log(js.status, js.failureReason || '');
    if (js.status === 'Completed') {
      console.log('VALIDATE_COMPLETED');
      return;
    }
    if (js.status === 'Failed' || js.status === 'Cancelled') throw new Error(JSON.stringify(js));
  }
  throw new Error('timeout');
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
