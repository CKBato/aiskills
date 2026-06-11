const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const wsId = 'b8a11494-b8e8-407f-af9c-5c360b143bd5';
const factNbId = 'ca68c384-99cd-4144-b559-14cbb07064df';
const factLogicalId = 'da5ea37e-5bf7-9eeb-48a9-ad5e09abf867';
const scriptsDir = __dirname;

const token = () =>
  JSON.parse(execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', { encoding: 'utf8' }))
    .accessToken;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const b64 = (s) => Buffer.from(s, 'utf8').toString('base64');

async function updateNotebook(nbId, logicalId, displayName, contentPath) {
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
  if (!r.ok) throw new Error(`update ${displayName} ${r.status} ${await r.text()}`);
  console.log('Updated notebook:', displayName);
}

async function runNotebook(nbId, label) {
  let t = token();
  console.log(`Starting ${label}...`);
  let r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/jobs/RunNotebook/instances`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: '{}',
  });
  if (r.status !== 202) throw new Error(`run ${label} ${r.status} ${await r.text()}`);
  const instanceId = r.headers.get('x-ms-job-id');
  console.log(`${label} job:`, instanceId);
  const pollUrl = `https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items/${nbId}/jobs/instances/${instanceId}`;
  for (let i = 0; i < 240; i++) {
    await sleep(15000);
    t = token();
    const st = await fetch(pollUrl, { headers: { Authorization: `Bearer ${t}` } });
    const js = await st.json();
    console.log(new Date().toISOString(), label, js.status, js.failureReason || '');
    if (js.status === 'Completed') {
      console.log(`${label}_COMPLETED`);
      return;
    }
    if (js.status === 'Failed' || js.status === 'Cancelled') throw new Error(`${label} failed: ${JSON.stringify(js)}`);
  }
  throw new Error(`${label} timeout`);
}

async function getOrCreateValidateNotebook() {
  let t = token();
  const list = await (await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items`, {
    headers: { Authorization: `Bearer ${t}` },
  })).json();
  let item = (list.value || []).find((i) => i.displayName === 'TIX-30119_PR1389_validate' && i.type === 'Notebook');
  if (item) return item.id;
  t = token();
  const r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      displayName: 'TIX-30119_PR1389_validate',
      type: 'Notebook',
      description: 'PR 1389 validation for aiagent-dev-workspace',
    }),
  });
  if (!r.ok) throw new Error(`create validate ${r.status} ${await r.text()}`);
  const created = await r.json();
  console.log('Created validate notebook:', created.id);
  return created.id;
}

(async () => {
  const factPath = path.join(scriptsDir, 'gold_customer_sales_fact-PR1389.py');
  const validatePath = path.join(scriptsDir, 'TIX-30119_PR1389_validate-content.py');

  await updateNotebook(factNbId, factLogicalId, 'gold_customer_sales_fact', factPath);
  await runNotebook(factNbId, 'gold_customer_sales_fact');

  const validateNbId = await getOrCreateValidateNotebook();
  await updateNotebook(validateNbId, validateNbId, 'TIX-30119_PR1389_validate', validatePath);
  await runNotebook(validateNbId, 'TIX-30119_PR1389_validate');

  console.log('PR1389_DEV_PIPELINE_COMPLETED');
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
