const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const wsId = process.argv[2];
const nbId = process.argv[3];
const label = process.argv[4] || nbId;

const token = () =>
  JSON.parse(execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', { encoding: 'utf8' }))
    .accessToken;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  let t = token();
  console.log(`Starting ${label}...`);
  let r = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/jobs/RunNotebook/instances`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
    body: '{}',
  });
  if (r.status !== 202) throw new Error(`run ${r.status} ${await r.text()}`);
  const instanceId = r.headers.get('x-ms-job-id') || r.headers.get('location')?.split('/').pop();
  console.log('Job instance:', instanceId);
  const pollUrl = `https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items/${nbId}/jobs/instances/${instanceId}`;
  for (let i = 0; i < 240; i++) {
    await sleep(15000);
    t = token();
    const st = await fetch(pollUrl, { headers: { Authorization: `Bearer ${t}` } });
    const js = await st.json();
    console.log(new Date().toISOString(), js.status, js.failureReason || '');
    if (js.status === 'Completed') {
      console.log('NOTEBOOK_COMPLETED');
      return;
    }
    if (js.status === 'Failed' || js.status === 'Cancelled') throw new Error(JSON.stringify(js));
  }
  throw new Error('timeout');
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
