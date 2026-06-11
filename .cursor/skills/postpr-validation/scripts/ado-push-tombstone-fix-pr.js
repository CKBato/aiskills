const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const org = 'Traffix-Data-Infrastructure';
const project = 'Traffix Medallion';
const repoId = '5c949b12-f510-4039-b9b7-59d6d5326875';
const branchName = 'feature/TIX-30027-tombstone-jdbc-batch';
const filePath = '/bronze_netsuite_incremental_load_batch_02.Notebook/notebook-content.py';
const contentPath = path.join(
  process.env.USERPROFILE,
  '.cursor/NFF/Inputs/TIX-16063/notebooks/bronze_netsuite_incremental_load_batch_02.Notebook/notebook-content.py'
);

const token = () =>
  JSON.parse(
    execSync('az account get-access-token -o json', {
      encoding: 'utf8',
    })
  ).accessToken;

const api = (p) => `https://dev.azure.com/${org}/${encodeURIComponent(project)}/_apis/git/repositories/${repoId}${p}`;

(async () => {
  const t = token();
  const headers = { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' };

  const refs = await fetch(`${api('/refs')}?filter=heads/${branchName}&api-version=7.1`, { headers });
  const branchOid = (await refs.json()).value[0].objectId;
  console.log('branch', branchOid);

  const content = fs.readFileSync(contentPath, 'utf8');
  const pushBody = {
    refUpdates: [{ name: `refs/heads/${branchName}`, oldObjectId: branchOid }],
    commits: [
      {
        comment: 'TIX-30027: batch tombstone_bronze_line_orphans JDBC queries under NetSuite QueryMaxSize',
        changes: [
          {
            changeType: 'edit',
            item: { path: filePath },
            newContent: { content, contentType: 'rawtext' },
          },
        ],
      },
    ],
  };

  const push = await fetch(`${api('/pushes')}?api-version=7.1`, {
    method: 'POST',
    headers,
    body: JSON.stringify(pushBody),
  });
  const pushJson = await push.json();
  if (!push.ok) throw new Error(`push failed: ${push.status} ${JSON.stringify(pushJson)}`);
  console.log('PUSH_OK', pushJson.commits?.[0]?.commitId || pushJson.pushId);

  const prBody = {
    sourceRefName: `refs/heads/${branchName}`,
    targetRefName: 'refs/heads/main',
    title: 'TIX-30027: Batch tombstone JDBC queries for NetSuite QueryMaxSize limit',
    description:
      '## Summary\n\nFixes prod bronze batch 02 failure when tombstone_bronze_line_orphans builds a single NetSuite JDBC IN clause exceeding SuiteAnalytics QueryMaxSize (32768).\n\n## Problem\n\nIncremental merge for transactionaccountingline succeeded, then tombstone failed when checking ~15K touched transactions (query length ~139K).\n\n## Fix\n\nBatch JDBC reads dynamically (max ~30K chars per query), union NS keys, then run orphan tombstone merge as before.\n\n## Jira\n\n- TIX-30027\n\n## Test plan\n\n- Merge PR\n- Git sync bronze_netsuite_batch_02\n- Re-run bronze batch 02; confirm multiple JDBC batch log lines and notebook completes',
  };

  const pr = await fetch(`${api('/pullrequests')}?api-version=7.1`, {
    method: 'POST',
    headers,
    body: JSON.stringify(prBody),
  });
  const prJson = await pr.json();
  if (!pr.ok) throw new Error(`pr failed: ${pr.status} ${JSON.stringify(prJson)}`);
  const url = `https://dev.azure.com/${org}/${encodeURIComponent(project)}/_git/Traffix%20Medallion%20Production/pullrequest/${prJson.pullRequestId}`;
  console.log('PR_URL', url);
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
