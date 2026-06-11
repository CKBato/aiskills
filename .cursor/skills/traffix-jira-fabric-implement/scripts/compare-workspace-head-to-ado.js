/**
 * G2 sync gate: compare Fabric workspace git head to ADO branch tip.
 *
 * Usage:
 *   node compare-workspace-head-to-ado.js "TIX-28531-Dimension-Undefined" "feature/TIX-29903-fix-home-currency-mrt-load"
 *   node compare-workspace-head-to-ado.js "TIX-28531-Dimension-Undefined" "main"
 *
 * Exit codes:
 *   0 — head matches ADO branch tip (workspace is latest for that branch)
 *   1 — usage / auth / fetch error
 *   2 — workspace not found
 *   3 — git not ConnectedAndInitialized
 *   4 — branch name mismatch (workspace on different branch than expected)
 *   5 — head behind or ahead of ADO tip (stale or unpushed workspace changes)
 */
const { execSync } = require('child_process');

process.env.NODE_OPTIONS = '--dns-result-order=ipv4first';

const workspaceName = process.argv[2];
const expectedBranch = process.argv[3];
const org = 'Traffix-Data-Infrastructure';
const project = 'd47186d3-ba21-45f6-8c83-14e351ea90e8';
const repoId = '5c949b12-f510-4039-b9b7-59d6d5326875';

if (!workspaceName || !expectedBranch) {
  console.error('Usage: node compare-workspace-head-to-ado.js <workspaceName> <adoBranchName>');
  console.error('  adoBranchName: feature/TIX-####-... or main (no refs/heads/ prefix)');
  process.exit(1);
}

const token = (resource) =>
  JSON.parse(execSync(`az account get-access-token --resource ${resource} -o json`, { encoding: 'utf8' }))
    .accessToken;

(async () => {
  const fabricToken = token('https://api.fabric.microsoft.com');
  const adoToken = token('https://app.vssps.visualstudio.com');

  const listRes = await fetch('https://api.fabric.microsoft.com/v1/workspaces', {
    headers: { Authorization: `Bearer ${fabricToken}` },
  });
  if (!listRes.ok) throw new Error(await listRes.text());
  const list = await listRes.json();
  const ws = (list.value || []).find((w) => w.displayName === workspaceName);
  if (!ws) {
    console.error(`Workspace not found: ${workspaceName}`);
    process.exit(2);
  }

  const connRes = await fetch(`https://api.fabric.microsoft.com/v1/workspaces/${ws.id}/git/connection`, {
    headers: { Authorization: `Bearer ${fabricToken}` },
  });
  const conn = connRes.ok ? await connRes.json() : null;
  if (!conn || conn.gitConnectionState !== 'ConnectedAndInitialized') {
    console.error('Git connection not ready:', conn?.gitConnectionState ?? 'missing');
    process.exit(3);
  }

  const workspaceBranch = conn.gitProviderDetails?.branchName;
  const workspaceHead = conn.gitSyncDetails?.head;
  const lastSyncTime = conn.gitSyncDetails?.lastSyncTime;

  if (workspaceBranch !== expectedBranch) {
    const out = {
      status: 'BRANCH_MISMATCH',
      workspaceName,
      workspaceId: ws.id,
      workspaceBranch,
      expectedBranch,
      workspaceHead,
      lastSyncTime,
      action: 'Switch workspace git branch to expected branch, then Update from Git in Fabric UI',
    };
    console.log(JSON.stringify(out, null, 2));
    process.exit(4);
  }

  const refUrl = `https://dev.azure.com/${org}/${project}/_apis/git/repositories/${repoId}/refs?filter=heads/${encodeURIComponent(expectedBranch)}&api-version=7.1`;
  const refRes = await fetch(refUrl, { headers: { Authorization: `Bearer ${adoToken}` } });
  if (!refRes.ok) throw new Error(`ADO ref fetch failed: ${await refRes.text()}`);
  const refBody = await refRes.json();
  const adoHead = refBody.value?.[0]?.objectId;

  if (!adoHead) {
    throw new Error(`ADO branch not found: ${expectedBranch}`);
  }

  const headMatch = workspaceHead === adoHead;
  const result = {
    status: headMatch ? 'SYNCED' : 'HEAD_MISMATCH',
    workspaceName,
    workspaceId: ws.id,
    branch: workspaceBranch,
    workspaceHead,
    adoHead,
    headMatch,
    lastSyncTime,
    recommendation: headMatch
      ? 'Workspace head matches ADO. Safe to run notebooks.'
      : 'Run Update from Git in Fabric (Source control). If still mismatched, check unpushed workspace commits.',
  };

  console.log(JSON.stringify(result, null, 2));
  if (!headMatch) process.exit(5);
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
