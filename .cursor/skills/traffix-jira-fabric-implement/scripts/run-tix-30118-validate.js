const fs = require('fs');
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const wsId = '5a2907f0-782e-4dd3-80c2-35e22d411d08';
const nbId = '6c13568c-4e50-4725-840c-5d04f77a555d';
const contentPath = require('path').join(__dirname, 'TIX-30118_cs-org_validate-content.py');

const content = fs.readFileSync(contentPath, 'utf8');
const platform = JSON.stringify(
  {
    $schema: 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json',
    metadata: { type: 'Notebook', displayName: 'TIX-30118_cs-org_validate' },
    config: { version: '2.0', logicalId: nbId },
  },
  null,
  2
);

const b64 = (s) => Buffer.from(s, 'utf8').toString('base64');
const token = () =>
  JSON.parse(execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', { encoding: 'utf8' }))
    .accessToken;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  let t = token();
  const body = {
    definition: {
      format: 'fabricGitSource',
      parts: [
        { path: 'notebook-content.py', payload: b64(content), payloadType: 'InlineBase64' },
        { path: '.platform', payload: b64(platform), payloadType: 'InlineBase64' },
      ],
    },
  };

  console.log('Updating notebook definition...');
  let r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/updateDefinition`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`updateDefinition ${r.status} ${await r.text()}`);
  console.log('Definition updated');

  console.log('Starting RunNotebook job...');
  r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/jobs/RunNotebook/instances`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: '{}',
  });
  if (r.status !== 202) throw new Error(`run ${r.status} ${await r.text()}`);
  const location = r.headers.get('location');
  const instanceId = r.headers.get('x-ms-job-id') || (location ? location.split('/').pop() : null);
  if (!instanceId) throw new Error(`run missing job id; location=${location}`);
  console.log('Job instance:', instanceId);

  const pollUrl = `https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items/${nbId}/jobs/instances/${instanceId}`;
  for (let i = 0; i < 180; i++) {
    await sleep(15000);
    t = token();
    const st = await fetch(pollUrl, { headers: { Authorization: `Bearer ${t}` } });
    if (!st.ok) throw new Error(`poll ${st.status} ${await st.text()}`);
    const js = await st.json();
    console.log('Status:', js.status, js.failureReason || '');
    if (js.status === 'Completed') {
      console.log('NOTEBOOK_COMPLETED');
      console.log(JSON.stringify(js, null, 2));
      return;
    }
    if (js.status === 'Failed' || js.status === 'Cancelled') {
      throw new Error(`Job failed: ${JSON.stringify(js)}`);
    }
  }
  throw new Error('Job timeout');
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
