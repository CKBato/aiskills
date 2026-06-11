const { execSync } = require('child_process');
process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';
const wsId = '5a2907f0-782e-4dd3-80c2-35e22d411d08';
const token = () => JSON.parse(execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', { encoding: 'utf8' })).accessToken;
const b64 = (s) => Buffer.from(s, 'utf8').toString('base64');
const content = `# Fabric notebook source\n\n# CELL ********************\n\n%run spark_configuration\n\n# CELL ********************\n\n%run silver_configuration\n\n# CELL ********************\n\ndf = spark.read.load(current_lakehouse_silver_abfss + '/Tables/tmp/tix30027_postprod_results')\ndf.show(truncate=False)\nfor r in df.collect():\n    print('METRIC|' + r.metric + '|' + r.value + '|' + r.status)\n`;
(async () => {
  const t = token();
  const items = (await (await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items?type=Notebook`, { headers: { Authorization: `Bearer ${t}` } })).json()).value;
  let nb = items.find((i) => i.displayName === 'TIX-30027_read_results');
  let nbId = nb?.id;
  if (!nbId) {
    const cr = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items`, { method: 'POST', headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ displayName: 'TIX-30027_read_results', type: 'Notebook' }) });
    nbId = (await cr.json()).id;
  }
  const platform = JSON.stringify({ $schema: 'https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json', metadata: { type: 'Notebook', displayName: 'TIX-30027_read_results' }, config: { version: '2.0', logicalId: nbId } }, null, 2);
  await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/updateDefinition`, { method: 'POST', headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ definition: { format: 'fabricGitSource', parts: [{ path: 'notebook-content.py', payload: b64(content), payloadType: 'InlineBase64' }, { path: '.platform', payload: b64(platform), payloadType: 'InlineBase64' }] } }) });
  const run = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${wsId}/notebooks/${nbId}/jobs/RunNotebook/instances`, { method: 'POST', headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' }, body: '{}' });
  const instanceId = run.headers.get('x-ms-job-id');
  const pollUrl = `https://api.fabric.microsoft.com/v1/workspaces/${wsId}/items/${nbId}/jobs/instances/${instanceId}`;
  for (let i = 0; i < 40; i++) {
    await new Promise((r) => setTimeout(r, 10000));
    const js = await (await fetch(pollUrl, { headers: { Authorization: `Bearer ${token()}` } })).json();
    if (js.status === 'Completed') { console.log('READ_DONE'); return; }
    if (js.status === 'Failed' || js.status === 'Cancelled') throw new Error(JSON.stringify(js));
  }
})().catch((e) => { console.error(e); process.exit(1); });
