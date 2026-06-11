const fs = require('fs');
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const org = 'Traffix-Data-Infrastructure';
const project = 'd47186d3-ba21-45f6-8c83-14e351ea90e8';
const repoId = '5c949b12-f510-4039-b9b7-59d6d5326875';

const [localFile, branch, oldObjectId, repoPath, message] = process.argv.slice(2);
if (!localFile || !branch || !oldObjectId || !repoPath || !message) {
  console.error('Usage: node ado-push-notebook.js <localFile> <branch> <oldObjectId> <repoPath> <message>');
  process.exit(1);
}

const token = () =>
  execSync('az account get-access-token --resource https://app.vssps.visualstudio.com -o json', { encoding: 'utf8' });
const accessToken = JSON.parse(token()).accessToken;
const b64 = fs.readFileSync(localFile).toString('base64');

const body = {
  refUpdates: [{ name: `refs/heads/${branch}`, oldObjectId }],
  commits: [
    {
      comment: message,
      changes: [
        {
          changeType: 'edit',
          item: { path: repoPath },
          newContent: { content: b64, contentType: 'base64encoded' },
        },
      ],
    },
  ],
};

(async () => {
  const url = `https://dev.azure.com/${org}/${project}/_apis/git/repositories/${repoId}/pushes?api-version=7.1`;
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`${r.status} ${text}`);
  console.log(text);
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
