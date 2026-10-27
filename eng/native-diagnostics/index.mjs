import { spawn } from 'node:child_process';
import { mkdir, open, readFile, writeFile, access } from 'node:fs/promises';
import path from 'node:path';

const suite = process.env.INPUT_SUITE;
if (!['provider', 'scale', 'aspire'].includes(suite)) throw new Error('Unknown native suite');
const root = process.env.GITHUB_WORKSPACE;
const resultRoot = path.join(root, 'artifacts/tests/windows-diagnostics', suite);
await mkdir(resultRoot, { recursive: true });
await mkdir(process.env.DCP_DIAGNOSTICS_LOG_FOLDER, { recursive: true });
const uploader = path.resolve(process.env.RUNNER_WORKSPACE, '../_actions/actions/upload-artifact/v4/dist/upload/index.js');
await access(uploader);
function exited(child) {
    return new Promise(resolve => {
        child.once('error', error => resolve({ code: null, error: String(error) }));
        child.once('close', (code, signal) => resolve({ code, signal }));
    });
}
async function command(executable, args, env = process.env) {
    const result = await exited(spawn(executable, args, { cwd: root, env, stdio: 'inherit' }));
    if (result.code !== 0) throw new Error(`${path.basename(executable)} exited ${JSON.stringify(result)}`);
}
const stdout = await open(path.join(resultRoot, 'stdout.log'), 'w');
const stderr = await open(path.join(resultRoot, 'stderr.log'), 'w');
const worker = spawn('pwsh', ['-NoProfile', '-File', path.join(root, 'eng/Run-NativeDiagnosticSuite.ps1'), '-Suite', suite],
    { cwd: root, stdio: ['ignore', stdout.fd, stderr.fd] });
let nativeOutcome;
const completion = exited(worker).then(result => { nativeOutcome = result; return result; });
await writeFile(path.join(resultRoot, 'worker.json'), JSON.stringify({
    processId: worker.pid, startedUtc: new Date().toISOString(), source: process.env.GITHUB_SHA,
}));
let diagnosticFailure;
try {
    for (const seconds of [120, 240, 360, 900, 1200]) {
        if (nativeOutcome) break;
        await command('pwsh', ['-NoProfile', '-File', path.join(root, 'eng/Save-NativeProcessCheckpoint.ps1'), '-Suite', suite, '-AtSeconds', String(seconds)]);
        await command(process.execPath, [uploader], {
            ...process.env, INPUT_NAME: `windows-${suite}-${seconds}s`,
            INPUT_PATH: path.join(resultRoot, `checkpoint-${seconds}`), 'INPUT_IF-NO-FILES-FOUND': 'error',
            INPUT_OVERWRITE: 'false', 'INPUT_INCLUDE-HIDDEN-FILES': 'false', 'INPUT_COMPRESSION-LEVEL': '6',
        });
    }
} catch (failure) {
    diagnosticFailure = failure;
} finally {
    const native = await completion;
    await stdout.close();
    await stderr.close();
    for (const name of ['stdout.log', 'stderr.log']) {
        const text = await readFile(path.join(resultRoot, name), 'utf8');
        console.log(text.split(/\r?\n/).slice(-100).join('\n'));
    }
    const result = JSON.parse(await readFile(path.join(resultRoot, 'result.json'), 'utf8'));
    if (result.exitCode !== native.code || native.code !== 0) throw new Error(`Native suite failed: ${JSON.stringify({ result, native })}`);
}
if (diagnosticFailure) throw diagnosticFailure;
if (suite === 'provider') await command('python', ['eng/verify-coverage.py', path.join(resultRoot, 'membership.cobertura.xml')]);
