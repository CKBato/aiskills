const fs = require('fs');
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';
const wsId = 'b8a11494-b8e8-407f-af9c-5c360b143bd5';
const validateNbId = '7ccf64c7-d7b6-42a9-b1f0-0b4d844a03e2';
const content = fs.readFileSync(__dirname + '/TIX-30119_PR1389_validate-content.py', 'utf8');
const token = () =>
  JSON.parse(execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', { encoding: 'utf8' }))
    .accessToken;
const b64 = (s) => Buffer.from(s, 'utf8').toString('base64');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  let t = token();
  const platform = JSON.stringify(
    {
      $schema: 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json',
      metadata: { type: 'Notebook', displayName: 'TIX-30119_PR1389_validate' },
      config: { version: '2.0', logicalId: validateNbId },
    },
    null,
    2
  );
  let r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${validateNbId}/updateDefinition`, {
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
  if (!r.ok) throw new Error(await r.text());
  r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${validateNbId}/jobs/RunNotebook/instances`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: '{}',
  });
  const instanceId = r.headers.get('x-ms-job-id');
  const pollUrl = `https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items/${validateNbId}/jobs/instances/${instanceId}`;
  for (let i = 0; i < 60; i++) {
    await sleep(15000);
    t = token();
    const js = await (await fetch(pollUrl, { headers: { Authorization: `Bearer ${t}` } })).json();
    console.log(js.status, js.failureReason || '');
    if (js.status === 'Completed') {
      console.log('VALIDATE_DONE');
      return;
    }
    if (js.status === 'Failed' || js.status === 'Cancelled') throw new Error(JSON.stringify(js));
  }
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
