/**
 * Verify a Fabric workspace exists and is connected to the expected ADO branch.
 *
 * Usage:
 *   node verify-workspace-git.js "TIX-29894-dim-user-employee-type" "feature/TIX-29894-dim-user-employee-type-fallback"
 */
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const workspaceName = process.argv[2];
const expectedBranch = process.argv[3];

if (!workspaceName) {
  console.error('Usage: node verify-workspace-git.js <workspaceName> [expectedBranch]');
  process.exit(1);
}

const token = () =>
  JSON.parse(
    execSync('az account get-access-token --resource https://api.fabric.microsoft.com -o json', {
      encoding: 'utf8',
    })
  ).accessToken;

(async () => {
  const t = token();
  const listRes = await fetch('https://api.fabric.microsoft.com/v1/workspaces', {
    headers: { Authorization: `Bearer ${t}` },
  });
  if (!listRes.ok) throw new Error(await listRes.text());
  const list = await listRes.json();
  const ws = (list.value || []).find((w) => w.displayName === workspaceName);
  if (!ws) {
    console.error(`Workspace not found: ${workspaceName}`);
    process.exit(2);
  }

  const connRes = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${ws.id}/git/connection`, {
    headers: { Authorization: `Bearer ${t}` },
  });
  const conn = connRes.ok ? await connRes.json() : null;

  const result = {
    workspaceId: ws.id,
    workspaceName: ws.displayName,
    capacityId: ws.capacityId,
    git: conn
      ? {
          state: conn.gitConnectionState,
          branch: conn.gitProviderDetails?.branchName,
          repository: conn.gitProviderDetails?.repositoryName,
          project: conn.gitProviderDetails?.projectName,
          head: conn.gitSyncDetails?.head,
          lastSyncTime: conn.gitSyncDetails?.lastSyncTime,
        }
      : null,
    branchMatch: expectedBranch
      ? conn?.gitProviderDetails?.branchName === expectedBranch
      : null,
  };

  console.log(JSON.stringify(result, null, 2));
  if (!conn || conn.gitConnectionState !== 'ConnectedAndInitialized') process.exit(3);
  if (expectedBranch && !result.branchMatch) process.exit(4);
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
