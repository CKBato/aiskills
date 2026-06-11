const fs = require('fs');
const https = require('https');
const { execSync } = require('child_process');

const org = 'Traffix-Data-Infrastructure';
const project = 'Traffix Medallion';
const repoId = '5c949b12-f510-4039-b9b7-59d6d5326875';
const branchName = 'feature/TIX-30027-tombstone-jdbc-batch';
const filePath = '/bronze_netsuite_incremental_load_batch_02.Notebook/notebook-content.py';
const contentPath = `${process.env.USERPROFILE}\\.cursor\\NFF\\Inputs\\TIX-16063\\notebooks\\bronze_netsuite_incremental_load_batch_02.Notebook\\notebook-content.py`;

const token = JSON.parse(execSync('az account get-access-token -o json', { encoding: 'utf8' })).accessToken;
const basePath = `/Traffix-Data-Infrastructure/${encodeURIComponent(project)}/_apis/git/repositories/${repoId}`;

function request(method, path, body) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const opts = {
      hostname: 'dev.azure.com',
      path,
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
        ...(data ? { 'Content-Length': Buffer.byteLength(data) } : {}),
      },
    };
    const req = https.request(opts, (res) => {
      let buf = '';
      res.on('data', (c) => (buf += c));
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(buf ? JSON.parse(buf) : {});
        } else {
          reject(new Error(`${method} ${path} -> ${res.statusCode}: ${buf.slice(0, 2000)}`));
        }
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

async function main() {
  const refs = await request('GET', `${basePath}/refs?filter=heads/${branchName}&api-version=7.1`);
  const branchOid = refs.value[0].objectId;
  console.log('branchOid', branchOid);

  const contentB64 = fs.readFileSync(contentPath).toString('base64');
  const push = await request('POST', `${basePath}/pushes?api-version=7.1`, {
    refUpdates: [{ name: `refs/heads/${branchName}`, oldObjectId: branchOid }],
    commits: [
      {
        comment: 'TIX-30027: batch tombstone_bronze_line_orphans JDBC queries under NetSuite QueryMaxSize',
        changes: [
          {
            changeType: 'edit',
            item: { path: filePath },
            newContent: { content: contentB64, contentType: 'base64encoded' },
          },
        ],
      },
    ],
  });
  console.log('PUSH_OK commit', push.commits[0].commitId);

  const pr = await request('POST', `${basePath}/pullrequests?api-version=7.1`, {
    sourceRefName: `refs/heads/${branchName}`,
    targetRefName: 'refs/heads/main',
    title: 'TIX-30027: Batch tombstone JDBC queries for NetSuite QueryMaxSize limit',
    description: [
      '## Summary',
      '',
      'Fixes prod bronze batch 02 failure when tombstone_bronze_line_orphans builds a single NetSuite JDBC IN clause exceeding SuiteAnalytics QueryMaxSize (32768).',
      '',
      '## Problem',
      '',
      'Incremental merge for transactionaccountingline succeeded, then tombstone failed when checking ~15K touched transactions (query length ~139K).',
      '',
      '## Fix',
      '',
      'Batch JDBC reads dynamically (max ~30K chars per query), union NS keys, then run orphan tombstone merge as before.',
      '',
      '## Jira',
      '',
      '- [TIX-30027](https://traffixsupport.atlassian.net/browse/TIX-30027)',
      '',
      '## Test plan',
      '',
      '- [ ] Merge PR',
      '- [ ] Git sync / deploy bronze_netsuite_batch_02',
      '- [ ] Re-run bronze batch 02; confirm multiple JDBC batch log lines and notebook completes',
    ].join('\n'),
  });

  const url = `https://dev.azure.com/${org}/Traffix%20Medallion/_git/Traffix%20Medallion%20Production/pullrequest/${pr.pullRequestId}`;
  console.log('PR_URL', url);
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
