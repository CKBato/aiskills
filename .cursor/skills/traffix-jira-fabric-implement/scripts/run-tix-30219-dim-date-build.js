const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const wsId = 'b8a11494-b8e8-407f-af9c-5c360b143bd5';
const nbName = 'TIX-30219_dim_date_build';
const contentPath = path.join(__dirname, 'TIX-30219_dim_date_build-content.py');
const content = fs.readFileSync(contentPath, 'utf8');

const token = () =>
  JSON.parse(execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', { encoding: 'utf8' }))
    .accessToken;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const b64 = (s) => Buffer.from(s, 'utf8').toString('base64');

(async () => {
  let t = token();

  const listRes = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items`, {
    headers: { Authorization: `Bearer ${t}` },
  });
  const list = await listRes.json();
  let item = (list.value || []).find((i) => i.displayName === nbName && i.type === 'Notebook');
  let nbId = item?.id;

  if (!nbId) {
    let r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        displayName: nbName,
        type: 'Notebook',
        description: 'TIX-30219 build dim_date with fixed default_* seed row',
      }),
    });
    if (!r.ok) throw new Error(`create ${r.status} ${await r.text()}`);
    item = await r.json();
    nbId = item.id;
    console.log('Created notebook:', nbId);
  } else {
    console.log('Reusing notebook:', nbId);
  }

  const platform = JSON.stringify(
    {
      $schema: 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json',
      metadata: { type: 'Notebook', displayName: nbName },
      config: { version: '2.0', logicalId: nbId },
    },
    null,
    2
  );

  let r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/updateDefinition`, {
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
  console.log('Notebook definition updated');

  r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/jobs/RunNotebook/instances`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: '{}',
  });
  if (r.status !== 202) throw new Error(`run ${r.status} ${await r.text()}`);
  const instanceId = r.headers.get('x-ms-job-id');
  console.log('Job instance:', instanceId);

  const pollUrl = `https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items/${nbId}/jobs/instances/${instanceId}`;
  for (let i = 0; i < 60; i++) {
    await sleep(15000);
    t = token();
    const st = await fetch(pollUrl, { headers: { Authorization: `Bearer ${t}` } });
    const js = await st.json();
    console.log(new Date().toISOString(), js.status, js.failureReason || '');
    if (js.status === 'Completed') {
      console.log('DIM_DATE_BUILD_COMPLETED');
      console.log('View table: lh_gold > Tables > dimension > dim_date');
      return;
    }
    if (js.status === 'Failed' || js.status === 'Cancelled') throw new Error(JSON.stringify(js));
  }
  throw new Error('timeout');
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
